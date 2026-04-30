// QueueModelTests.swift
// Tests for the playback queue's pure data layer: ordered episode keys,
// position management, addNext/addLast semantics, reorder, dedup.
//
// Scope: playhead-05i (Queue / Up Next Management). The queue's data
// model is intentionally exercised in isolation here — persistence
// integration lives in `QueueServiceTests.swift`, auto-advance lives
// in `QueueAutoAdvanceTests.swift`.

import Foundation
import Testing
@testable import Playhead

@Suite("QueueModel — pure data structure")
struct QueueModelTests {

    @Test("empty queue has no entries and no peek")
    func emptyQueueIsEmpty() {
        var queue = PlaybackQueueModel()
        #expect(queue.entries.isEmpty)
        #expect(queue.peek == nil)
        #expect(queue.popNext() == nil)
    }

    @Test("addLast on empty queue places entry at position 0")
    func addLastOnEmpty() {
        var queue = PlaybackQueueModel()
        queue.addLast(episodeKey: "ep-A")
        #expect(queue.entries.map(\.episodeKey) == ["ep-A"])
        #expect(queue.peek?.episodeKey == "ep-A")
    }

    @Test("addLast appends in order")
    func addLastAppends() {
        var queue = PlaybackQueueModel()
        queue.addLast(episodeKey: "ep-A")
        queue.addLast(episodeKey: "ep-B")
        queue.addLast(episodeKey: "ep-C")
        #expect(queue.entries.map(\.episodeKey) == ["ep-A", "ep-B", "ep-C"])
    }

    @Test("addNext on empty queue places entry at position 0")
    func addNextOnEmpty() {
        var queue = PlaybackQueueModel()
        queue.addNext(episodeKey: "ep-A")
        #expect(queue.entries.map(\.episodeKey) == ["ep-A"])
    }

    @Test("addNext on populated queue inserts at front")
    func addNextInsertsFirst() {
        var queue = PlaybackQueueModel()
        queue.addLast(episodeKey: "ep-A")
        queue.addLast(episodeKey: "ep-B")
        queue.addNext(episodeKey: "ep-X")
        #expect(queue.entries.map(\.episodeKey) == ["ep-X", "ep-A", "ep-B"])
    }

    @Test("popNext removes and returns the front entry")
    func popNextReturnsAndRemoves() {
        var queue = PlaybackQueueModel()
        queue.addLast(episodeKey: "ep-A")
        queue.addLast(episodeKey: "ep-B")

        let popped = queue.popNext()
        #expect(popped?.episodeKey == "ep-A")
        #expect(queue.entries.map(\.episodeKey) == ["ep-B"])
    }

    @Test("popNext on empty queue returns nil")
    func popNextOnEmpty() {
        var queue = PlaybackQueueModel()
        #expect(queue.popNext() == nil)
    }

    @Test("remove deletes the named entry and compacts positions")
    func removeCompacts() {
        var queue = PlaybackQueueModel()
        queue.addLast(episodeKey: "ep-A")
        queue.addLast(episodeKey: "ep-B")
        queue.addLast(episodeKey: "ep-C")
        queue.remove(episodeKey: "ep-B")
        #expect(queue.entries.map(\.episodeKey) == ["ep-A", "ep-C"])
        // Positions should be compacted to [0, 1] — no gap.
        #expect(queue.entries.map(\.position) == [0, 1])
    }

    @Test("remove of unknown key is a no-op")
    func removeUnknown() {
        var queue = PlaybackQueueModel()
        queue.addLast(episodeKey: "ep-A")
        queue.remove(episodeKey: "nope")
        #expect(queue.entries.map(\.episodeKey) == ["ep-A"])
    }

    @Test("clear empties the queue")
    func clearEmpties() {
        var queue = PlaybackQueueModel()
        queue.addLast(episodeKey: "ep-A")
        queue.addLast(episodeKey: "ep-B")
        queue.clear()
        #expect(queue.entries.isEmpty)
        #expect(queue.peek == nil)
    }

    @Test("reorder moves an entry to a new position")
    func reorderMoves() {
        var queue = PlaybackQueueModel()
        queue.addLast(episodeKey: "ep-A")
        queue.addLast(episodeKey: "ep-B")
        queue.addLast(episodeKey: "ep-C")
        queue.move(fromOffsets: IndexSet(integer: 0), toOffset: 3)
        #expect(queue.entries.map(\.episodeKey) == ["ep-B", "ep-C", "ep-A"])
        #expect(queue.entries.map(\.position) == [0, 1, 2])
    }

    @Test("addLast on duplicate key moves entry to new tail position")
    func dedupOnAddLast() {
        var queue = PlaybackQueueModel()
        queue.addLast(episodeKey: "ep-A")
        queue.addLast(episodeKey: "ep-B")
        queue.addLast(episodeKey: "ep-A") // duplicate
        // Expected: ep-A moves to the tail; queue stays ["ep-B", "ep-A"]
        #expect(queue.entries.map(\.episodeKey) == ["ep-B", "ep-A"])
        #expect(queue.entries.map(\.position) == [0, 1])
    }

    @Test("addNext on duplicate key moves entry to front")
    func dedupOnAddNext() {
        var queue = PlaybackQueueModel()
        queue.addLast(episodeKey: "ep-A")
        queue.addLast(episodeKey: "ep-B")
        queue.addNext(episodeKey: "ep-B") // duplicate
        // Expected: ep-B moves to the head; queue is ["ep-B", "ep-A"]
        #expect(queue.entries.map(\.episodeKey) == ["ep-B", "ep-A"])
        #expect(queue.entries.map(\.position) == [0, 1])
    }

    @Test("queue count reflects entries")
    func countReflects() {
        var queue = PlaybackQueueModel()
        #expect(queue.count == 0)
        queue.addLast(episodeKey: "ep-A")
        queue.addLast(episodeKey: "ep-B")
        #expect(queue.count == 2)
    }
}
