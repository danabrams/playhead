// DesignTokenColorsTests.swift
// Verifies the Quiet Instrument palette resolves to exact sRGB components,
// semantic aliases resolve in both color schemes, and text-on-background
// combinations meet WCAG AA contrast.

import XCTest
import SwiftUI
import UIKit
@testable import Playhead

final class DesignTokenColorsTests: XCTestCase {

    // MARK: - Raw palette hex verification

    func testInkHexMatches() {
        assertComponents(UIColor(Palette.ink), r: 0x0E, g: 0x11, b: 0x16)
    }

    func testCharcoalHexMatches() {
        assertComponents(UIColor(Palette.charcoal), r: 0x1A, g: 0x1F, b: 0x27)
    }

    func testBoneHexMatches() {
        assertComponents(UIColor(Palette.bone), r: 0xF3, g: 0xEE, b: 0xE4)
    }

    func testCopperHexMatches() {
        assertComponents(UIColor(Palette.copper), r: 0xC9, g: 0x6A, b: 0x3D)
    }

    func testMutedSageHexMatches() {
        assertComponents(UIColor(Palette.mutedSage), r: 0x8C, g: 0x9B, b: 0x90)
    }

    func testSoftSteelHexMatches() {
        assertComponents(UIColor(Palette.softSteel), r: 0x95, g: 0xA0, b: 0xAE)
    }

    // MARK: - Semantic aliases resolve in both modes

    func testAllSemanticAliasesResolveInLightAndDark() {
        let aliases: [(String, Color)] = [
            ("background",      AppColors.background),
            ("surface",         AppColors.surface),
            ("surfaceElevated", AppColors.surfaceElevated),
            ("textPrimary",     AppColors.textPrimary),
            ("textSecondary",   AppColors.textSecondary),
            ("textTertiary",    AppColors.textTertiary),
            ("accent",          AppColors.accent),
            ("accentSubtle",    AppColors.accentSubtle)
        ]
        let lightTraits = UITraitCollection(userInterfaceStyle: .light)
        let darkTraits  = UITraitCollection(userInterfaceStyle: .dark)
        for (name, color) in aliases {
            let ui = UIColor(color)
            let light = ui.resolvedColor(with: lightTraits)
            let dark  = ui.resolvedColor(with: darkTraits)
            XCTAssertNotNil(light.cgColor, "\(name) failed to resolve in light mode")
            XCTAssertNotNil(dark.cgColor,  "\(name) failed to resolve in dark mode")
        }
    }

    // MARK: - WCAG contrast

    func testCopperOnInkMeetsWCAGAA() {
        let ratio = contrastRatio(UIColor(Palette.copper), UIColor(Palette.ink))
        XCTAssertGreaterThanOrEqual(ratio, 4.5,
            "Copper-on-Ink contrast ratio \(ratio) < WCAG AA 4.5")
    }

    func testBoneOnInkMeetsWCAGAA() {
        let ratio = contrastRatio(UIColor(Palette.bone), UIColor(Palette.ink))
        XCTAssertGreaterThanOrEqual(ratio, 4.5,
            "Bone-on-Ink contrast ratio \(ratio) < WCAG AA 4.5")
    }

    func testBoneOnCharcoalMeetsWCAGAA() {
        let ratio = contrastRatio(UIColor(Palette.bone), UIColor(Palette.charcoal))
        XCTAssertGreaterThanOrEqual(ratio, 4.5,
            "Bone-on-Charcoal contrast ratio \(ratio) < WCAG AA 4.5")
    }

    // MARK: - Elevation invariant
    //
    // surfaceElevated must NEVER be darker than surface in either mode.
    // Elevation in this design system reads as "lifted toward the viewer";
    // a darker elevated surface flips that mental model and confuses every
    // future caller. The light-mode value used to be Bone (darker than the
    // base white surface) — that was the bug fixed in M4.

    func testSurfaceElevationLuminanceMonotonicLight() {
        let lightTraits = UITraitCollection(userInterfaceStyle: .light)
        let surface = UIColor(AppColors.surface).resolvedColor(with: lightTraits)
        let elevated = UIColor(AppColors.surfaceElevated).resolvedColor(with: lightTraits)
        XCTAssertGreaterThanOrEqual(
            relativeLuminance(elevated),
            relativeLuminance(surface),
            "surfaceElevated must be at least as bright as surface in light mode "
            + "(elevation = lifted toward viewer = lighter)"
        )
    }

    func testSurfaceElevationLuminanceMonotonicDark() {
        let darkTraits = UITraitCollection(userInterfaceStyle: .dark)
        let surface = UIColor(AppColors.surface).resolvedColor(with: darkTraits)
        let elevated = UIColor(AppColors.surfaceElevated).resolvedColor(with: darkTraits)
        XCTAssertGreaterThanOrEqual(
            relativeLuminance(elevated),
            relativeLuminance(surface),
            "surfaceElevated must be at least as bright as surface in dark mode"
        )
    }

    // MARK: - Light/dark component assertions for every semantic alias
    //
    // Acceptance criterion #1: "All 6 colors render correctly in both light
    // and dark mode." The earlier `testAllSemanticAliasesResolveInLightAndDark`
    // only asserted "doesn't crash" — these tests assert the actual sRGB
    // components match the expected raw palette values per the bead spec.

    func testBackgroundResolvesToBoneInLightAndInkInDark() {
        assertResolved(AppColors.background, light: 0xF3EEE4, dark: 0x0E1116)
    }

    func testSurfaceResolvesToBoneInLightAndCharcoalInDark() {
        assertResolved(AppColors.surface, light: 0xF3EEE4, dark: 0x1A1F27)
    }

    func testSurfaceElevatedResolvesToWhiteInLightAndLiftedCharcoalInDark() {
        assertResolved(AppColors.surfaceElevated, light: 0xFFFFFF, dark: 0x252B35)
    }

    func testTextPrimaryResolvesToInkInLightAndBoneInDark() {
        assertResolved(AppColors.textPrimary, light: 0x0E1116, dark: 0xF3EEE4)
    }

    func testTextSecondaryResolvesToMutedSageInLightAndSoftSteelInDark() {
        assertResolved(AppColors.textSecondary, light: 0x8C9B90, dark: 0x95A0AE)
    }

    func testAccentResolvesToCopperInBothModes() {
        assertResolved(AppColors.accent, light: 0xC96A3D, dark: 0xC96A3D)
    }

    /// `accentSubtle` is `Palette.copper.opacity(0.16)` and is intentionally
    /// non-text. Verify the underlying components are still copper and the
    /// alpha is the configured 16%.
    func testAccentSubtleIsCopperAt16PercentAlpha() {
        let ui = UIColor(AppColors.accentSubtle)
        var r: CGFloat = 0, g: CGFloat = 0, b: CGFloat = 0, a: CGFloat = 0
        XCTAssertTrue(ui.getRed(&r, green: &g, blue: &b, alpha: &a))
        XCTAssertEqual(r, 0xC9 / 255.0, accuracy: 1.0 / 255.0 + 0.001)
        XCTAssertEqual(g, 0x6A / 255.0, accuracy: 1.0 / 255.0 + 0.001)
        XCTAssertEqual(b, 0x3D / 255.0, accuracy: 1.0 / 255.0 + 0.001)
        XCTAssertEqual(a, 0.16, accuracy: 0.005)
    }

    /// `textTertiary` is `softSteel.opacity(0.8)` in light and full softSteel
    /// in dark. The opacity prevents a clean component-equality comparison so
    /// we assert the underlying RGB matches softSteel and check alpha
    /// separately.
    func testTextTertiaryIsSoftSteelInBothModesWithLightOpacity() {
        let lightTraits = UITraitCollection(userInterfaceStyle: .light)
        let darkTraits  = UITraitCollection(userInterfaceStyle: .dark)
        let light = UIColor(AppColors.textTertiary).resolvedColor(with: lightTraits)
        let dark  = UIColor(AppColors.textTertiary).resolvedColor(with: darkTraits)
        var r: CGFloat = 0, g: CGFloat = 0, b: CGFloat = 0, a: CGFloat = 0
        XCTAssertTrue(light.getRed(&r, green: &g, blue: &b, alpha: &a))
        XCTAssertEqual(r, 0x95 / 255.0, accuracy: 1.0 / 255.0 + 0.001)
        XCTAssertEqual(g, 0xA0 / 255.0, accuracy: 1.0 / 255.0 + 0.001)
        XCTAssertEqual(b, 0xAE / 255.0, accuracy: 1.0 / 255.0 + 0.001)
        XCTAssertEqual(a, 0.8, accuracy: 0.005)
        var r2: CGFloat = 0, g2: CGFloat = 0, b2: CGFloat = 0, a2: CGFloat = 0
        XCTAssertTrue(dark.getRed(&r2, green: &g2, blue: &b2, alpha: &a2))
        XCTAssertEqual(r2, 0x95 / 255.0, accuracy: 1.0 / 255.0 + 0.001)
        XCTAssertEqual(g2, 0xA0 / 255.0, accuracy: 1.0 / 255.0 + 0.001)
        XCTAssertEqual(b2, 0xAE / 255.0, accuracy: 1.0 / 255.0 + 0.001)
        XCTAssertEqual(a2, 1.0, accuracy: 0.005)
    }

    // MARK: - Helpers

    /// Resolve `color` in light + dark traits and assert each side matches
    /// the given hex (alpha must be 1.0).
    private func assertResolved(_ color: Color, light: UInt32, dark: UInt32,
                                file: StaticString = #filePath, line: UInt = #line) {
        let lightTraits = UITraitCollection(userInterfaceStyle: .light)
        let darkTraits  = UITraitCollection(userInterfaceStyle: .dark)
        let lightUI = UIColor(color).resolvedColor(with: lightTraits)
        let darkUI  = UIColor(color).resolvedColor(with: darkTraits)
        assertHexComponents(lightUI, hex: light, file: file, line: line)
        assertHexComponents(darkUI,  hex: dark,  file: file, line: line)
    }

    private func assertHexComponents(_ color: UIColor, hex: UInt32,
                                     file: StaticString = #filePath, line: UInt = #line) {
        let r = Int((hex >> 16) & 0xFF)
        let g = Int((hex >> 8) & 0xFF)
        let b = Int(hex & 0xFF)
        var rf: CGFloat = 0, gf: CGFloat = 0, bf: CGFloat = 0, af: CGFloat = 0
        XCTAssertTrue(color.getRed(&rf, green: &gf, blue: &bf, alpha: &af),
                      "Color not in sRGB", file: file, line: line)
        let tolerance: CGFloat = 1.0 / 255.0 + 0.001
        XCTAssertEqual(rf, CGFloat(r) / 255.0, accuracy: tolerance,
                       "red mismatch (expected #\(String(hex, radix: 16)))",
                       file: file, line: line)
        XCTAssertEqual(gf, CGFloat(g) / 255.0, accuracy: tolerance,
                       "green mismatch (expected #\(String(hex, radix: 16)))",
                       file: file, line: line)
        XCTAssertEqual(bf, CGFloat(b) / 255.0, accuracy: tolerance,
                       "blue mismatch (expected #\(String(hex, radix: 16)))",
                       file: file, line: line)
        XCTAssertEqual(af, 1.0, accuracy: 0.005,
                       "alpha must be 1.0 for hex resolution", file: file, line: line)
    }

    private func assertComponents(_ color: UIColor, r: Int, g: Int, b: Int,
                                  file: StaticString = #filePath, line: UInt = #line) {
        var rf: CGFloat = 0, gf: CGFloat = 0, bf: CGFloat = 0, af: CGFloat = 0
        XCTAssertTrue(color.getRed(&rf, green: &gf, blue: &bf, alpha: &af),
                      "Color not in sRGB", file: file, line: line)
        let tolerance: CGFloat = 1.0 / 255.0 + 0.001
        XCTAssertEqual(rf, CGFloat(r) / 255.0, accuracy: tolerance, file: file, line: line)
        XCTAssertEqual(gf, CGFloat(g) / 255.0, accuracy: tolerance, file: file, line: line)
        XCTAssertEqual(bf, CGFloat(b) / 255.0, accuracy: tolerance, file: file, line: line)
    }

    /// WCAG 2.1 relative luminance from sRGB components (0...1).
    private func relativeLuminance(_ color: UIColor) -> Double {
        var r: CGFloat = 0, g: CGFloat = 0, b: CGFloat = 0, a: CGFloat = 0
        color.getRed(&r, green: &g, blue: &b, alpha: &a)
        func channel(_ c: CGFloat) -> Double {
            let v = Double(c)
            return v <= 0.03928 ? v / 12.92 : pow((v + 0.055) / 1.055, 2.4)
        }
        return 0.2126 * channel(r) + 0.7152 * channel(g) + 0.0722 * channel(b)
    }

    private func contrastRatio(_ a: UIColor, _ b: UIColor) -> Double {
        let la = relativeLuminance(a)
        let lb = relativeLuminance(b)
        let lighter = max(la, lb)
        let darker  = min(la, lb)
        return (lighter + 0.05) / (darker + 0.05)
    }
}
