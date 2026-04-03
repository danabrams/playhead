// DownloadManager.swift
// Background download management for offline episode storage.
// Model downloads are handled separately by AssetProvider.

import Foundation
import OSLog

/// Manages background downloads for podcast episode audio.
///
/// Model asset downloads are handled by ``AssetProvider`` — this
/// manager is only for user-facing episode downloads and caching.
actor DownloadManager {
    private let logger = Logger(subsystem: "com.playhead", category: "Downloads")
}
