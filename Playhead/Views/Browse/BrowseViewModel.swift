// BrowseViewModel.swift
// Drives search state for BrowseView with debounced queries.

import Foundation
import SwiftUI

@MainActor
@Observable
final class BrowseViewModel {

    // MARK: - State

    var searchText = ""
    var results: [DiscoveryResult] = []
    var isSearching = false
    var showError = false
    var errorMessage = ""

    // MARK: - Services

    let discoveryService: PodcastDiscoveryService

    // MARK: - Init

    init(discoveryService: PodcastDiscoveryService = PodcastDiscoveryService()) {
        self.discoveryService = discoveryService
    }

    // MARK: - Debounce

    /// Minimum characters before a search fires.
    private static let minQueryLength = 2

    /// Debounce interval in seconds.
    private static let debounceInterval: TimeInterval = 0.4

    private var searchTask: Task<Void, Never>?

    // MARK: - Search

    func debounceSearch(query: String) {
        searchTask?.cancel()

        let trimmed = query.trimmingCharacters(in: .whitespacesAndNewlines)

        if trimmed.count < Self.minQueryLength {
            results = []
            isSearching = false
            return
        }

        isSearching = true

        searchTask = Task {
            // Debounce delay
            try? await Task.sleep(for: .milliseconds(Int(Self.debounceInterval * 1000)))

            guard !Task.isCancelled else { return }

            do {
                let searchResults = try await discoveryService.searchPodcasts(
                    query: trimmed,
                    limit: 25
                )
                guard !Task.isCancelled else { return }
                results = searchResults
            } catch {
                guard !Task.isCancelled else { return }
                errorMessage = error.localizedDescription
                showError = true
            }

            isSearching = false
        }
    }
}
