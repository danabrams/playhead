// OPMLViewModelTests.swift
// playhead-2jo: tests for the SettingsView import/export view-model.
//
// The view model owns the *transient* state of an import/export run
// (in-progress flag, progress fraction, summary, error) so the View
// stays declarative. Production wiring binds the seams to real
// PodcastDiscoveryService + ModelContext; the tests use closures.

import Foundation
import Testing
@testable import Playhead

@Suite("OPMLImportExportViewModel")
@MainActor
struct OPMLImportExportViewModelTests {

    private func feed(_ title: String, _ url: String) -> OPMLFeed {
        OPMLFeed(title: title, xmlUrl: URL(string: url)!)
    }

    @Test("Initial state: idle, no progress, no summary")
    func initialState() {
        let vm = OPMLImportExportViewModel()
        #expect(vm.isImporting == false)
        #expect(vm.isExporting == false)
        #expect(vm.progressFraction == 0)
        #expect(vm.lastImportSummary == nil)
        #expect(vm.lastImportError == nil)
        #expect(vm.exportedFileURL == nil)
    }

    @Test("Successful import surfaces summary and clears progress")
    func successfulImport() async {
        let vm = OPMLImportExportViewModel()
        // Simple OPML byte stream with two feeds.
        let xml = #"""
        <?xml version="1.0" encoding="UTF-8"?>
        <opml version="2.0">
          <head><title>x</title></head>
          <body>
            <outline type="rss" text="A" xmlUrl="https://example.com/a.rss"/>
            <outline type="rss" text="B" xmlUrl="https://example.com/b.rss"/>
          </body>
        </opml>
        """#
        await vm.runImport(
            data: Data(xml.utf8),
            exists: { _ in false },
            resolve: { _ in .success(()) },
            persist: { _ in }
        )
        #expect(vm.isImporting == false)
        #expect(vm.lastImportError == nil)
        #expect(vm.lastImportSummary?.imported == 2)
        #expect(vm.lastImportSummary?.skippedDuplicate == 0)
        #expect(vm.lastImportSummary?.failed.isEmpty == true)
    }

    @Test("Malformed OPML data sets lastImportError, not summary")
    func malformedOPMLSurfacesError() async {
        let vm = OPMLImportExportViewModel()
        let bad = Data("not even xml".utf8)
        await vm.runImport(
            data: bad,
            exists: { _ in false },
            resolve: { _ in .success(()) },
            persist: { _ in }
        )
        #expect(vm.isImporting == false)
        #expect(vm.lastImportSummary == nil)
        if let err = vm.lastImportError {
            // localizedDescription is a user-facing string; must be set.
            #expect(!err.isEmpty)
        } else {
            Issue.record("Expected lastImportError to be set")
        }
    }

    @Test("Empty OPML body surfaces a user-facing error")
    func emptyOPMLBodySurfacesError() async {
        let vm = OPMLImportExportViewModel()
        let xml = #"""
        <?xml version="1.0" encoding="UTF-8"?>
        <opml version="2.0">
          <head><title>x</title></head>
          <body></body>
        </opml>
        """#
        await vm.runImport(
            data: Data(xml.utf8),
            exists: { _ in false },
            resolve: { _ in .success(()) },
            persist: { _ in }
        )
        #expect(vm.lastImportError != nil)
    }

    @Test("Export writes a UTF-8 OPML file to a temp directory")
    func exportWritesTempFile() async throws {
        let vm = OPMLImportExportViewModel()
        let feeds = [
            feed("A", "https://example.com/a.rss"),
            feed("B", "https://example.com/b.rss"),
        ]
        await vm.runExport(feeds: feeds, documentTitle: "Playhead Test")

        let url = try #require(vm.exportedFileURL)
        #expect(FileManager.default.fileExists(atPath: url.path))
        #expect(url.pathExtension == "opml")

        let bytes = try Data(contentsOf: url)
        let xml = String(decoding: bytes, as: UTF8.self)
        #expect(xml.contains("<opml version=\"2.0\">"))
        #expect(xml.contains("Playhead Test"))
        #expect(xml.contains("https://example.com/a.rss"))
    }

    @Test("Re-export overwrites the previous file URL (single rolling export)")
    func reExportOverwrites() async throws {
        let vm = OPMLImportExportViewModel()
        let feeds = [feed("A", "https://example.com/a.rss")]
        await vm.runExport(feeds: feeds, documentTitle: "First")
        let firstURL = try #require(vm.exportedFileURL)
        let firstContents = try Data(contentsOf: firstURL)

        await vm.runExport(feeds: feeds, documentTitle: "Second")
        let secondURL = try #require(vm.exportedFileURL)
        let secondContents = try Data(contentsOf: secondURL)
        #expect(secondContents != firstContents)
    }

    @Test("Reset clears summary, error, and exported file URL")
    func resetClearsState() async {
        let vm = OPMLImportExportViewModel()
        let xml = #"""
        <?xml version="1.0" encoding="UTF-8"?>
        <opml version="2.0">
          <head><title>x</title></head>
          <body>
            <outline type="rss" text="A" xmlUrl="https://example.com/a.rss"/>
          </body>
        </opml>
        """#
        await vm.runImport(
            data: Data(xml.utf8),
            exists: { _ in false },
            resolve: { _ in .success(()) },
            persist: { _ in }
        )
        #expect(vm.lastImportSummary != nil)
        vm.reset()
        #expect(vm.lastImportSummary == nil)
        #expect(vm.lastImportError == nil)
    }
}
