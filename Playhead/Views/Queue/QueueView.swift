// QueueView.swift
// Sheet UI for the user's playback queue ("Up Next"). Reads from
// `QueueViewModel`; user actions (drag-reorder, swipe-to-remove,
// clear) route through the VM, which mutates `PlaybackQueueService`.
//
// Design tokens follow the rest of the app:
//   * Title: AppTypography.headline (sans)
//   * Subtitle: AppTypography.caption (sans, secondary)
//   * Duration: AppTypography.timestamp (mono, tertiary)
//   * Background: AppColors.background
//
// Empty state copy: "Queue is empty. Add an episode from your library
// to start a queue." — neutral and direct, matching the bead spec
// intent without committing to specific motion or affordance copy
// (e.g. "swipe an episode") that may not match the real swipe action.

import SwiftUI

struct QueueView: View {

    @State var viewModel: QueueViewModel
    @State private var showClearConfirmation: Bool = false
    @Environment(\.dismiss) private var dismiss

    var body: some View {
        NavigationStack {
            ZStack {
                AppColors.background.ignoresSafeArea()
                if viewModel.rows.isEmpty {
                    emptyState
                } else {
                    queueList
                }
            }
            .navigationTitle("Up Next")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .topBarLeading) {
                    Button("Done") { dismiss() }
                        .foregroundStyle(AppColors.textSecondary)
                }
                if !viewModel.rows.isEmpty {
                    ToolbarItem(placement: .topBarTrailing) {
                        Button(role: .destructive) {
                            showClearConfirmation = true
                        } label: {
                            Text("Clear")
                                .foregroundStyle(AppColors.accent)
                        }
                        .accessibilityLabel("Clear queue")
                    }
                }
            }
            .confirmationDialog(
                "Clear the queue?",
                isPresented: $showClearConfirmation,
                titleVisibility: .visible
            ) {
                Button("Clear queue", role: .destructive) {
                    Task {
                        try? await viewModel.clear()
                        await viewModel.refresh()
                    }
                }
                Button("Cancel", role: .cancel) {}
            } message: {
                Text("This removes every episode from your Up Next list. The episodes themselves stay in your library.")
            }
        }
        .task { await viewModel.refresh() }
    }

    // MARK: - Subviews

    private var queueList: some View {
        List {
            ForEach(viewModel.rows) { row in
                QueueRow(row: row)
                    .listRowBackground(AppColors.background)
            }
            .onDelete { indexSet in
                let keysToRemove = indexSet.map { viewModel.rows[$0].episodeKey }
                Task {
                    for key in keysToRemove {
                        try? await viewModel.remove(episodeKey: key)
                    }
                    await viewModel.refresh()
                }
            }
            .onMove { source, destination in
                Task {
                    try? await viewModel.move(fromOffsets: source, toOffset: destination)
                    await viewModel.refresh()
                }
            }
        }
        .listStyle(.plain)
        .scrollContentBackground(.hidden)
        .environment(\.editMode, .constant(.active))
    }

    private var emptyState: some View {
        VStack(spacing: Spacing.md) {
            Image(systemName: "list.bullet.rectangle")
                .font(.system(size: 36, weight: .light))
                .foregroundStyle(AppColors.textTertiary)
            Text("Queue is empty")
                .font(AppTypography.headline)
                .foregroundStyle(AppColors.textSecondary)
            Text("Add an episode from your library to start a queue.")
                .font(AppTypography.caption)
                .foregroundStyle(AppColors.textTertiary)
                .multilineTextAlignment(.center)
                .padding(.horizontal, Spacing.xl)
        }
    }
}

// MARK: - Row

private struct QueueRow: View {
    let row: QueueDisplayRow

    var body: some View {
        VStack(alignment: .leading, spacing: Spacing.xs) {
            Text(row.title)
                .font(AppTypography.headline)
                .foregroundStyle(AppColors.textPrimary)
                .lineLimit(2)
            HStack(spacing: Spacing.sm) {
                if let podcastTitle = row.podcastTitle {
                    Text(podcastTitle)
                        .font(AppTypography.caption)
                        .foregroundStyle(AppColors.textSecondary)
                        .lineLimit(1)
                }
                if let duration = row.duration {
                    Text(TimeFormatter.formatDuration(duration))
                        .font(AppTypography.timestamp)
                        .foregroundStyle(AppColors.textTertiary)
                }
            }
        }
        .padding(.vertical, Spacing.xxs)
        .accessibilityElement(children: .combine)
        .accessibilityLabel(accessibilityLabel)
    }

    private var accessibilityLabel: String {
        var parts: [String] = [row.title]
        if let podcastTitle = row.podcastTitle { parts.append(podcastTitle) }
        if let duration = row.duration {
            parts.append(TimeFormatter.formatDuration(duration))
        }
        return parts.joined(separator: ", ")
    }
}
