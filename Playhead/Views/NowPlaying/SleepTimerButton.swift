//
//  SleepTimerButton.swift
//  Playhead
//
//  playhead-g21: the moon in the transport row and the duration picker it
//  presents. Soft Steel when idle, Copper with a countdown badge when running.
//

import SwiftData
import SwiftUI

struct SleepTimerButton: View {
    let state: SleepTimerState
    let action: () -> Void

    var body: some View {
        Button(action: action) {
            VStack(spacing: 2) {
                Image(systemName: state.isArmed ? "moon.fill" : "moon")
                    .font(.system(size: 20, weight: .medium))
                    .foregroundStyle(state.isArmed ? Palette.copper : Palette.softSteel)
                badge
            }
            .frame(minWidth: 44, minHeight: 44)
        }
        .buttonStyle(.plain)
        .accessibilityLabel(accessibilityText)
    }

    @ViewBuilder
    private var badge: some View {
        switch state {
        case .running(let fireAt):
            TimelineView(.periodic(from: .now, by: 1)) { context in
                Text(Self.countdownText(until: fireAt, now: context.date))
                    .font(AppTypography.timestamp)
                    .foregroundStyle(Palette.copper)
                    .monospacedDigit()
            }
        case .untilEndOfEpisode:
            Text("End")
                .font(AppTypography.timestamp)
                .foregroundStyle(Palette.copper)
        case .fadingOut:
            Text("0:00")
                .font(AppTypography.timestamp)
                .foregroundStyle(Palette.copper)
        case .idle, .paused:
            EmptyView()
        }
    }

    private var accessibilityText: String {
        switch state {
        case .idle, .paused: return "Sleep timer"
        case .running(let fireAt): return "Sleep timer, \(Self.countdownText(until: fireAt, now: Date())) remaining"
        case .untilEndOfEpisode: return "Sleep timer, until the end of the episode"
        case .fadingOut: return "Sleep timer, pausing"
        }
    }

    /// `mm:ss`, or `h:mm:ss` past an hour. Never negative.
    static func countdownText(until fireAt: Date, now: Date) -> String {
        let remaining = max(Int(fireAt.timeIntervalSince(now).rounded(.up)), 0)
        let hours = remaining / 3600, minutes = (remaining % 3600) / 60, seconds = remaining % 60
        if hours > 0 { return String(format: "%d:%02d:%02d", hours, minutes, seconds) }
        return String(format: "%d:%02d", minutes, seconds)
    }
}

struct SleepTimerSheet: View {
    let state: SleepTimerState
    let onPick: (SleepDuration) -> Void
    let onCancel: () -> Void
    @Environment(\.dismiss) private var dismiss
    @Environment(\.modelContext) private var modelContext
    @Query private var allPreferences: [UserPreferences]

    private var remembered: SleepDuration? {
        allPreferences.first?.defaultSleepDuration.flatMap(SleepDuration.init(rawName:))
    }

    var body: some View {
        NavigationStack {
            List {
                Section {
                    ForEach(SleepDuration.allCases, id: \.self) { duration in
                        Button {
                            remember(duration)
                            onPick(duration)
                            dismiss()
                        } label: {
                            HStack {
                                Text(duration.label)
                                    .font(AppTypography.body)
                                    .foregroundStyle(AppColors.textPrimary)
                                Spacer()
                                if duration == remembered {
                                    Image(systemName: "checkmark")
                                        .foregroundStyle(Palette.copper)
                                        .accessibilityLabel("Last used")
                                }
                            }
                        }
                    }
                } header: {
                    Text("Pause playback after")
                }
                if state.isArmed {
                    Section {
                        Button(role: .destructive) {
                            onCancel()
                            dismiss()
                        } label: {
                            Text("Cancel timer")
                        }
                    }
                }
            }
            .navigationTitle("Sleep Timer")
            .navigationBarTitleDisplayMode(.inline)
        }
        .presentationDetents([.medium])
    }

    private func remember(_ duration: SleepDuration) {
        if let preferences = allPreferences.first {
            preferences.defaultSleepDuration = duration.rawName
        }
        try? modelContext.save()
    }
}
