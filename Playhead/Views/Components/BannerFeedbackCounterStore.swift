// BannerFeedbackCounterStore.swift
// Local aggregate-only learning labels for ad banners (playhead-jw63.1).

import Foundation

/// The complete durable banner-feedback aggregate.
///
/// This intentionally contains only three anonymous counters. It carries no
/// episode, show, window, device, transcript, audio, timestamp, or event data.
/// A future consumer may read this snapshot, but this store never uploads it.
struct BannerFeedbackCounts: Codable, Equatable, Sendable {
    let bannersShown: Int
    let bannersConfirmed: Int
    let bannersDenied: Int

    static let zero = BannerFeedbackCounts(
        bannersShown: 0,
        bannersConfirmed: 0,
        bannersDenied: 0
    )

    fileprivate var normalized: BannerFeedbackCounts {
        BannerFeedbackCounts(
            bannersShown: max(0, bannersShown),
            bannersConfirmed: max(0, bannersConfirmed),
            bannersDenied: max(0, bannersDenied)
        )
    }
}

/// A one-tap answer to the question presented on every ad banner.
enum BannerFeedbackResponse: Sendable, Equatable {
    case confirmed
    case denied
}

/// Main-actor-isolated, UserDefaults-backed aggregate counter store.
///
/// All reads and read-modify-writes happen on the main actor, matching the
/// banner queue's execution context. The persisted value is one small JSON
/// object containing exactly `BannerFeedbackCounts`; there is no event stream
/// and no network behavior.
@MainActor
final class BannerFeedbackCounterStore {
    static let shared = BannerFeedbackCounterStore()

    private let defaults: UserDefaults
    private let storageKey: String

    init(
        defaults: UserDefaults = .standard,
        storageKey: String = "playhead.bannerFeedback.aggregate.v1"
    ) {
        self.defaults = defaults
        self.storageKey = storageKey
    }

    var snapshot: BannerFeedbackCounts {
        guard let data = defaults.data(forKey: storageKey),
              let decoded = try? JSONDecoder().decode(
                  BannerFeedbackCounts.self,
                  from: data
              )
        else {
            return .zero
        }
        return decoded.normalized
    }

    func recordBannerShown() {
        update { counts in
            BannerFeedbackCounts(
                bannersShown: Self.incremented(counts.bannersShown),
                bannersConfirmed: counts.bannersConfirmed,
                bannersDenied: counts.bannersDenied
            )
        }
    }

    func recordConfirmed() {
        update { counts in
            BannerFeedbackCounts(
                bannersShown: counts.bannersShown,
                bannersConfirmed: Self.incremented(counts.bannersConfirmed),
                bannersDenied: counts.bannersDenied
            )
        }
    }

    func recordDenied() {
        update { counts in
            BannerFeedbackCounts(
                bannersShown: counts.bannersShown,
                bannersConfirmed: counts.bannersConfirmed,
                bannersDenied: Self.incremented(counts.bannersDenied)
            )
        }
    }

    private func update(
        _ mutation: (BannerFeedbackCounts) -> BannerFeedbackCounts
    ) {
        let updated = mutation(snapshot)
        guard let data = try? JSONEncoder().encode(updated) else { return }
        defaults.set(data, forKey: storageKey)
    }

    private static func incremented(_ value: Int) -> Int {
        value == Int.max ? Int.max : value + 1
    }
}
