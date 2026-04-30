// PlaybackQueueService.swift
// Actor that owns the user-facing playback queue. Wraps the SwiftData
// `QueueEntry` rows behind a small CRUD-and-peek API; auto-advance
// integration with `PlaybackService` lives in
// `PlaybackQueueAutoAdvancer.swift` (next bead-cycle commit).
//
// Threading: the service is an actor so concurrent UI mutations
// (Library swipe-actions on main, drag-reorder, popNext from
// auto-advance) serialize through one queue and never observe a
// half-mutated state. SwiftData's `ModelContext` is NOT thread-safe;
// this actor holds a private context built from the injected
// `ModelContainer` and never hands it out.
//
// Persistence semantics: every public mutation calls `save()` so a
// force-quit immediately after the call retains the new ordering.
// Cold-launch behavior is "queue intact, no auto-play" — `popNext` is
// only invoked from `PlaybackQueueAutoAdvancer` when an episode
// finishes, never on app start.

import Foundation
import SwiftData

/// Public DTO for queue rows. We expose this Sendable struct rather than
/// the SwiftData `QueueEntry` itself so callers across actor boundaries
/// don't have to worry about model thread-safety.
struct PlaybackQueueRow: Sendable, Hashable, Identifiable {
    let episodeKey: String
    let position: Int
    let addedAt: Date

    var id: String { episodeKey }
}

actor PlaybackQueueService {

    private let modelContainer: ModelContainer
    /// Lazily-instantiated context: SwiftData logs an "Unbinding from
    /// the main queue" warning when a `ModelContext` constructed on the
    /// main thread is later used off the main thread. Building the
    /// context lazily inside `withContext()` ensures it is born on the
    /// actor's executor and never bound to main in the first place.
    private var _context: ModelContext?

    init(modelContainer: ModelContainer) {
        self.modelContainer = modelContainer
    }

    private var context: ModelContext {
        if let existing = _context { return existing }
        let fresh = ModelContext(modelContainer)
        _context = fresh
        return fresh
    }

    // MARK: - Reads

    /// Front entry (lowest position), or `nil` when the queue is empty.
    func peek() -> PlaybackQueueRow? {
        let rows = (try? fetchOrdered()) ?? []
        return rows.first.map(Self.row(from:))
    }

    /// All queued rows sorted by position ascending.
    func allEntries() throws -> [PlaybackQueueRow] {
        try fetchOrdered().map(Self.row(from:))
    }

    /// Number of rows currently in the queue.
    var count: Int {
        let rows = (try? fetchOrdered()) ?? []
        return rows.count
    }

    /// Whether `episodeKey` is queued.
    func contains(episodeKey: String) -> Bool {
        let rows = (try? fetchOrdered()) ?? []
        return rows.contains { $0.episodeKey == episodeKey }
    }

    // MARK: - Writes

    /// Insert at the head. If the key already exists, move it to position 0
    /// (no duplicate row). Persists.
    func addNext(episodeKey: String, addedAt: Date = .now) throws {
        var model = try loadModel()
        model.addNext(episodeKey: episodeKey, addedAt: addedAt)
        try writeModel(model)
    }

    /// Append at the tail. If the key already exists, move it to the tail
    /// (no duplicate row). Persists.
    func addLast(episodeKey: String, addedAt: Date = .now) throws {
        var model = try loadModel()
        model.addLast(episodeKey: episodeKey, addedAt: addedAt)
        try writeModel(model)
    }

    /// Pop the head row (returns it as a Sendable DTO). Persists the
    /// shorter queue. Returns `nil` when the queue is empty.
    @discardableResult
    func popNext() throws -> PlaybackQueueRow? {
        var model = try loadModel()
        guard let popped = model.popNext() else { return nil }
        try writeModel(model)
        return PlaybackQueueRow(
            episodeKey: popped.episodeKey,
            position: popped.position,
            addedAt: popped.addedAt
        )
    }

    /// Delete the row whose episodeKey matches; compact positions. No-op
    /// when the key is not present.
    func remove(episodeKey: String) throws {
        var model = try loadModel()
        model.remove(episodeKey: episodeKey)
        try writeModel(model)
    }

    /// Empty the queue.
    func clear() throws {
        var model = PlaybackQueueModel()
        model.clear()
        try writeModel(model)
    }

    /// Drag-reorder shim mirroring SwiftUI `List.onMove`.
    func move(fromOffsets source: IndexSet, toOffset destination: Int) throws {
        var model = try loadModel()
        model.move(fromOffsets: source, toOffset: destination)
        try writeModel(model)
    }

    // MARK: - Persistence helpers

    /// Fetch all rows ordered by position ascending.
    private func fetchOrdered() throws -> [QueueEntry] {
        var descriptor = FetchDescriptor<QueueEntry>()
        descriptor.sortBy = [SortDescriptor(\QueueEntry.position, order: .forward)]
        return try context.fetch(descriptor)
    }

    /// Hydrate a `PlaybackQueueModel` from persistence so we can apply
    /// pure mutations and write the result back as the canonical state.
    private func loadModel() throws -> PlaybackQueueModel {
        let rows = try fetchOrdered()
        var model = PlaybackQueueModel()
        for row in rows {
            // Use addLast so positions renumber from scratch even if
            // the persisted rows had gaps. The dedup behavior in addLast
            // would only fire if a key were duplicated, which violates
            // an invariant the service maintains — but it's a safe
            // recovery path.
            model.addLast(episodeKey: row.episodeKey, addedAt: row.addedAt)
        }
        return model
    }

    /// Write a `PlaybackQueueModel` back to SwiftData. Strategy:
    /// delete-all-then-insert-all so the persisted state always exactly
    /// mirrors the in-memory model. Volumes are tiny (a queue is
    /// realistically <100 episodes), so the simpler delete-and-rewrite
    /// path is preferred over diff-and-update.
    private func writeModel(_ model: PlaybackQueueModel) throws {
        let existing = try fetchOrdered()
        for row in existing {
            context.delete(row)
        }
        for entry in model.entries {
            context.insert(QueueEntry(
                episodeKey: entry.episodeKey,
                position: entry.position,
                addedAt: entry.addedAt
            ))
        }
        try context.save()
    }

    /// Bridge a SwiftData `QueueEntry` into the Sendable DTO callers see.
    private static func row(from entry: QueueEntry) -> PlaybackQueueRow {
        PlaybackQueueRow(
            episodeKey: entry.episodeKey,
            position: entry.position,
            addedAt: entry.addedAt
        )
    }
}
