// OPMLSummaryHeadlineTests.swift
// playhead-2jo: pin the user-facing headline copy for the import-result row.

import Foundation
import Testing
import SwiftUI
@testable import Playhead

@Suite("OPML – Summary Headline Copy")
@MainActor
struct OPMLSummaryHeadlineTests {

    private func headline(_ result: OPMLImportResult) -> String {
        let view = SettingsView()
        return view.opmlSummaryHeadline(result)
    }

    private func failure(_ url: String, _ reason: String) -> OPMLImportResult.Failure {
        OPMLImportResult.Failure(url: URL(string: url)!, reason: reason)
    }

    @Test("All-success singular: 'Imported 1 podcast.'")
    func allSuccessSingular() {
        let r = OPMLImportResult(imported: 1, skippedDuplicate: 0, failed: [])
        #expect(headline(r) == "Imported 1 podcast.")
    }

    @Test("All-success plural: 'Imported N podcasts.'")
    func allSuccessPlural() {
        let r = OPMLImportResult(imported: 32, skippedDuplicate: 0, failed: [])
        #expect(headline(r) == "Imported 32 podcasts.")
    }

    @Test("With duplicates only: 'Imported X, Y already in library.'")
    func withDuplicates() {
        let r = OPMLImportResult(imported: 4, skippedDuplicate: 2, failed: [])
        #expect(headline(r) == "Imported 4, 2 already in library.")
    }

    @Test("With failures only: 'Imported X. Y could not be found.'")
    func withFailures() {
        let r = OPMLImportResult(
            imported: 32,
            skippedDuplicate: 0,
            failed: [failure("https://example.com/a.rss", "404")]
        )
        #expect(headline(r) == "Imported 32. 1 could not be found.")
    }

    @Test("With duplicates and failures: combined headline")
    func withDuplicatesAndFailures() {
        let r = OPMLImportResult(
            imported: 5,
            skippedDuplicate: 2,
            failed: [failure("https://x", "y"), failure("https://q", "z")]
        )
        #expect(headline(r) == "Imported 5, 2 duplicate, 2 failed.")
    }
}
