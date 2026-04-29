// DebugEpisodeExporter.swift
// UI-side Transferable value for the debug-export ShareLink. The
// fetch + format pipeline lives in `DebugEpisodeExportService`
// (under `Playhead/Services/Diagnostics/`) so this file does not
// touch persistence/analysis types — see playhead-fwvz.

#if DEBUG

import Foundation
import CoreTransferable
import UniformTypeIdentifiers

// MARK: - DebugEpisodeExport

/// A Transferable document containing a text-format episode analysis report.
/// Used with ShareLink to export from the app. The underlying content is
/// produced by `DebugEpisodeExportService` and handed to this struct as
/// already-resolved text — no store / analysis types are referenced here.
struct DebugEpisodeExport: Transferable, Sendable {
    let content: String
    let filename: String

    static var transferRepresentation: some TransferRepresentation {
        DataRepresentation(exportedContentType: .plainText) { export in
            Data(export.content.utf8)
        }
        .suggestedFileName { $0.filename }
    }
}

#endif
