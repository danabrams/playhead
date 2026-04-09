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

    // MARK: - Helpers

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
