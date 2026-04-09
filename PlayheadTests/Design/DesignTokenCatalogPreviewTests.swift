// DesignTokenCatalogPreviewTests.swift
// Renders the DesignTokenCatalog view at iPhone SE width and verifies that
// (a) it doesn't crash, (b) it produces a substantial bitmap, and
// (c) Dynamic Type at AX3 actually expands the layout — i.e. labels are
// not truncating, they are growing.
//
// Width: iPhone SE 3rd gen portrait = 375pt. The repo's deployment target
// is iOS 26 (project.yml), which excludes SE 1st/2nd gen entirely, so 375
// is the strictest meaningful "smallest supported screen" width.

import XCTest
import SwiftUI
@testable import Playhead

@MainActor
final class DesignTokenCatalogPreviewTests: XCTestCase {

    private static let iPhoneSEWidth: CGFloat = 375
    /// Lower bound on the AX3 rendered height. The catalog enumerates ~30+
    /// rows (colors, type roles, spacing, radii, motion, haptics). At AX3
    /// each row is well above 32pt, so a healthy render must clear ~1200pt.
    /// If a future change accidentally collapses the layout (truncation,
    /// missing tokens, broken stacking) the height will fall below this
    /// floor and this test will fail loudly.
    private static let ax3MinimumRenderedHeight: CGFloat = 1200
    /// AX3 must produce a substantially taller layout than the default
    /// dynamic type size — if labels were truncating, AX3 height would
    /// stay flat. A 1.4x ratio is conservative; in practice the increase
    /// is much larger.
    private static let ax3VsDefaultGrowthRatio: CGFloat = 1.4

    func testCatalogRendersAtIPhoneSEWidthWithSubstantialContent() throws {
        let height = try renderedHeight(dynamicType: .accessibility3)
        XCTAssertGreaterThanOrEqual(
            height,
            Self.ax3MinimumRenderedHeight,
            "DesignTokenCatalog rendered too short at AX3 (\(height)pt < \(Self.ax3MinimumRenderedHeight)pt). "
            + "This usually means labels are truncating or tokens are missing from the catalog."
        )
    }

    func testCatalogExpandsUnderDynamicType() throws {
        let defaultHeight = try renderedHeight(dynamicType: .large)
        let ax3Height = try renderedHeight(dynamicType: .accessibility3)

        XCTAssertGreaterThanOrEqual(
            ax3Height,
            defaultHeight * Self.ax3VsDefaultGrowthRatio,
            "AX3 height (\(ax3Height)pt) did not grow enough vs default (\(defaultHeight)pt). "
            + "Expected at least \(Self.ax3VsDefaultGrowthRatio)x growth — anything less suggests labels are truncating instead of scaling."
        )
    }

    /// Render the catalog at the given Dynamic Type size and return the
    /// natural fitting height for the iPhone SE width. Uses
    /// `UIHostingController.sizeThatFits(in:)` so the measurement reflects
    /// the actual layout pass, not just whatever bitmap `ImageRenderer`
    /// happens to produce.
    private func renderedHeight(dynamicType: DynamicTypeSize) throws -> CGFloat {
        let view = DesignTokenCatalog()
            .environment(\.dynamicTypeSize, dynamicType)
        let host = UIHostingController(rootView: view)
        host.view.frame = CGRect(
            x: 0, y: 0,
            width: Self.iPhoneSEWidth,
            height: .greatestFiniteMagnitude
        )
        let fitted = host.sizeThatFits(in: CGSize(
            width: Self.iPhoneSEWidth,
            height: .greatestFiniteMagnitude
        ))
        return fitted.height
    }
}
