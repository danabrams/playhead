// QueueEntry.swift
// SwiftData model for one row in the user's playback queue (playhead-05i).
//
// Stored separately from `Episode.queuePosition`: that field is reserved
// for the Activity tab's "Up Next" analysis-priority ordering (playhead-
// cjqq), which is a different concern (which episode the analysis
// pipeline should work on next, not which episode plays next). Mixing
// the two onto the same column would couple analysis-scheduling and
// playback-queue UI to the same write path and produce surprising
// behavior on the Activity screen when a user reorders the playback
// queue.
//
// `episodeKey` references `Episode.canonicalEpisodeKey`. We store the
// key (a String) rather than a SwiftData @Relationship because the
// queue ordering needs to survive even if the referenced Episode row
// is briefly absent (e.g. during a feed refresh that drops the row
// before re-inserting it). A consumer that fails to resolve an
// `episodeKey` to a live Episode treats the queue entry as a tombstone
// and skips it.

import Foundation
import SwiftData

@Model
final class QueueEntry {
    /// References `Episode.canonicalEpisodeKey`. Unique within the queue —
    /// a key cannot appear twice (PlaybackQueueService dedups before
    /// insert; tests assert the invariant).
    var episodeKey: String

    /// 0-based ordering index. The service maintains the invariant
    /// `entries[i].position == i` after every mutation; consumers can sort
    /// rows by `position` ascending to retrieve the playback order.
    var position: Int

    /// Wall-clock timestamp at which the user added this episode to the
    /// queue. Not load-bearing for ordering (position is) but useful for
    /// diagnostics and potential "added X minutes ago" UI affordances.
    var addedAt: Date

    init(episodeKey: String, position: Int, addedAt: Date = .now) {
        self.episodeKey = episodeKey
        self.position = position
        self.addedAt = addedAt
    }
}
