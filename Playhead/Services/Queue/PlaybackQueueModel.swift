// PlaybackQueueModel.swift
// Pure value-typed playback queue used by `PlaybackQueueService` as its
// in-memory source of truth. The model deliberately knows nothing about
// SwiftData, persistence, or PlaybackService — its only job is to maintain
// an ordered list of `(episodeKey, position)` entries that satisfies the
// queue semantics the service exposes:
//
//   * `addNext` inserts at position 0 (front)
//   * `addLast` appends at position N
//   * `popNext` removes and returns the front entry
//   * `remove(episodeKey:)` deletes a row by key, compacting positions
//   * `move(fromOffsets:toOffset:)` reorders, renumbering positions
//   * `clear` empties
//
// Duplicate semantics (decided 2026-04-29 for playhead-05i):
//   * `addNext(K)` where K is already queued — move K to the head; do NOT
//     produce two rows. Idempotent for a key that's already at position 0.
//   * `addLast(K)` where K is already queued — move K to the tail; do NOT
//     produce two rows. Idempotent for a key that's already at the tail.
//
// Positions are recomputed from the entry order on every mutation so the
// invariant `entries[i].position == i` always holds; consumers can rely on
// that for sort.

import Foundation

/// One row in the playback queue. Carries the canonical episode key (so
/// the consumer can resolve to a SwiftData `Episode` row), the position
/// index, and the wall-clock timestamp at which the entry was inserted.
struct PlaybackQueueEntry: Sendable, Hashable {
    let episodeKey: String
    let position: Int
    let addedAt: Date
}

/// Pure value-typed queue — see file header for invariants.
struct PlaybackQueueModel: Sendable, Equatable {

    private(set) var entries: [PlaybackQueueEntry] = []

    /// Front entry (position 0), or `nil` when empty.
    var peek: PlaybackQueueEntry? {
        entries.first
    }

    /// Number of queued entries.
    var count: Int {
        entries.count
    }

    /// Insert `episodeKey` at the head. If the key already exists, move it
    /// to position 0 (no duplicate row).
    mutating func addNext(episodeKey: String, addedAt: Date = .now) {
        // Remove the existing entry (if any), then insert at index 0.
        // This satisfies both "new key" and "dedup-move-to-front" cases.
        entries.removeAll { $0.episodeKey == episodeKey }
        entries.insert(
            PlaybackQueueEntry(episodeKey: episodeKey, position: 0, addedAt: addedAt),
            at: 0
        )
        renumber()
    }

    /// Append `episodeKey` at the tail. If the key already exists, move it
    /// to the tail (no duplicate row).
    mutating func addLast(episodeKey: String, addedAt: Date = .now) {
        entries.removeAll { $0.episodeKey == episodeKey }
        entries.append(
            PlaybackQueueEntry(
                episodeKey: episodeKey,
                position: entries.count,
                addedAt: addedAt
            )
        )
        renumber()
    }

    /// Remove and return the head entry. Returns `nil` when empty.
    /// Compacts remaining positions so `entries[i].position == i`.
    @discardableResult
    mutating func popNext() -> PlaybackQueueEntry? {
        guard !entries.isEmpty else { return nil }
        let popped = entries.removeFirst()
        renumber()
        return popped
    }

    /// Delete the entry whose `episodeKey` matches; compact positions.
    /// No-op if the key is not present.
    mutating func remove(episodeKey: String) {
        let originalCount = entries.count
        entries.removeAll { $0.episodeKey == episodeKey }
        if entries.count != originalCount {
            renumber()
        }
    }

    /// Empty the queue.
    mutating func clear() {
        entries.removeAll()
    }

    /// Drag-reorder shim that mirrors SwiftUI's `List.onMove` signature.
    /// Renumbers positions afterward.
    mutating func move(fromOffsets source: IndexSet, toOffset destination: Int) {
        entries.move(fromOffsets: source, toOffset: destination)
        renumber()
    }

    /// Returns true when `episodeKey` is currently queued.
    func contains(episodeKey: String) -> Bool {
        entries.contains { $0.episodeKey == episodeKey }
    }

    // MARK: - Internal

    /// Rebuild `position` to match each entry's current index. Called after
    /// every mutation so the invariant `entries[i].position == i` holds.
    private mutating func renumber() {
        entries = entries.enumerated().map { index, entry in
            PlaybackQueueEntry(
                episodeKey: entry.episodeKey,
                position: index,
                addedAt: entry.addedAt
            )
        }
    }
}
