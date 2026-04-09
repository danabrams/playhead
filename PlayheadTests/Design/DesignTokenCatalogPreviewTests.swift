// DesignTokenCatalogPreviewTests.swift
// Renders the DesignTokenCatalog view at iPhone SE width / AX3 to verify
// it doesn't crash and produces a non-empty image (smoke test for the
// preview surface that exercises every design token).

import XCTest
import SwiftUI
@testable import Playhead

@MainActor
final class DesignTokenCatalogPreviewTests: XCTestCase {

    func testCatalogRendersAtIPhoneSEWidthAtAX3() throws {
        // iPhone SE (3rd gen) portrait width is 375pt. Constrain the catalog
        // to that width and render it at AX3 dynamic type.
        let view = DesignTokenCatalog()
            .frame(width: 375)
            .environment(\.dynamicTypeSize, .accessibility3)

        let renderer = ImageRenderer(content: view)
        renderer.proposedSize = ProposedViewSize(width: 375, height: nil)
        let image = renderer.uiImage
        XCTAssertNotNil(image, "DesignTokenCatalog failed to render at AX3 / iPhone SE width")
        guard let image else { return }
        XCTAssertGreaterThan(image.size.width, 0)
        XCTAssertGreaterThan(image.size.height, 0)
    }
}
