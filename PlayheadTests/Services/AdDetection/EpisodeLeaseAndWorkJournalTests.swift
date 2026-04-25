// EpisodeLeaseAndWorkJournalTests.swift
// playhead-uzdq: validates the per-episode execution lease on
// analysis_jobs, the append-only WorkJournal, the global monotonic
// SchedulerEpoch singleton, and orphan recovery on cold launch.
//
// The bead's acceptance matrix (copied from the bead spec):
//   1. Lease acquisition on fresh episode -> row has matching
//      {leaseOwner, generationID, schedulerEpoch}; WorkJournal has
//      exactly one `acquired` entry.
//   2. Two concurrent acquireLease on the same episode -> one succeeds,
//      one throws LeaseHeld.
//   3. renewLease with stale epoch -> throws StaleEpoch; original lease
//      untouched.
//   4. Duplicate releaseLease(finalized) same generationID -> second is
//      no-op (idempotent); exactly one `finalized` entry in journal.
//   5. Orphan recovery: expiresAt=now-15s, last event `checkpointed` ->
//      requeued with fresh generationID, epoch bumped, attemptCount=0,
//      lane preserved.
//   6. Orphan recovery Now-lane row stale >60 s -> demoted to Soon
//      (priority 10).
//   7. Crash-injection: transaction rollback leaves DB consistent.
//
// The orphan-recovery tests call the store-level primitives directly
// and replay the coordinator's `recoverOrphans` policy via a local
// helper. The coordinator wraps these primitives but adds no behavior
// that is not already covered by the store contract — so keeping the
// tests at the store layer avoids constructing a full
// AnalysisCoordinator (which would require ~6 collaborator services).

import Foundation
import SQLite3
import Testing
@testable import Playhead

@Suite("Episode execution lease + WorkJournal (playhead-uzdq)")
struct EpisodeLeaseAndWorkJournalTests {

    // MARK: - Migration shape

    @Test("Fresh DB: analysis_jobs carries generationID + schedulerEpoch columns")
    func freshDbAddsLeaseColumnsToAnalysisJobs() async throws {
        let dir = try makeTempDir(prefix: "uzdq-columns")
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        _ = store

        #expect(try probeColumnExists(in: dir, table: "analysis_jobs", column: "generationID"))
        #expect(try probeColumnExists(in: dir, table: "analysis_jobs", column: "schedulerEpoch"))
    }

    @Test("Fresh DB: work_journal table + indexes present")
    func freshDbCreatesWorkJournalTableAndIndexes() async throws {
        let dir = try makeTempDir(prefix: "uzdq-journal-ddl")
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        _ = store

        #expect(try probeTableExists(in: dir, table: "work_journal"))
        #expect(try probeIndexExists(in: dir, indexName: "idx_wj_episode_gen"))
        #expect(try probeIndexExists(in: dir, indexName: "idx_wj_epoch"))
    }

    @Test("Fresh DB: scheduler_epoch singleton seeded at 1")
    func freshDbSeedsSchedulerEpoch() async throws {
        let store = try await makeTestStore()
        let epoch = try await store.fetchSchedulerEpoch()
        #expect(epoch == 1)
    }

    @Test("Double migrate(): lease columns not duplicated")
    func doubleMigrateDoesNotDuplicateLeaseColumns() async throws {
        let dir = try makeTempDir(prefix: "uzdq-idempotent")
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        AnalysisStore.resetMigratedPathsForTesting()
        try await store.migrate()

        #expect(try countColumn(in: dir, table: "analysis_jobs", column: "generationID") == 1)
        #expect(try countColumn(in: dir, table: "analysis_jobs", column: "schedulerEpoch") == 1)
    }

    // MARK: - Acceptance 1: fresh acquire

    @Test("Acquire on fresh episode: row + journal reflect the new lease")
    func acquireOnFreshEpisodeWritesLeaseAndJournal() async throws {
        let store = try await makeTestStore()
        let episodeId = "ep-acq"
        try await store.insertJob(makeAnalysisJob(jobId: "j1", episodeId: episodeId, workKey: "wk1"))

        let epoch = try await store.fetchSchedulerEpoch() ?? 0
        let gen = UUID()
        let now: Double = 1_000_000
        let descriptor = try await store.acquireEpisodeLease(
            episodeId: episodeId,
            ownerWorkerId: "worker-1",
            generationID: gen.uuidString,
            schedulerEpoch: epoch,
            now: now,
            ttlSeconds: 30
        )
        #expect(descriptor.jobId == "j1")
        #expect(descriptor.ownerWorkerId == "worker-1")

        let fetched = try await store.fetchJob(byId: "j1")
        #expect(fetched?.leaseOwner == "worker-1")
        #expect(fetched?.generationID == gen.uuidString)
        #expect(fetched?.schedulerEpoch == epoch)
        #expect(fetched?.leaseExpiresAt == now + 30)

        let entries = try await store.fetchWorkJournalEntries(
            episodeId: episodeId,
            generationID: gen.uuidString
        )
        #expect(entries.count == 1)
        #expect(entries.first?.eventType == .acquired)
        #expect(entries.first?.schedulerEpoch == epoch)
        #expect(entries.first?.artifactClass == .scratch)
    }

    // MARK: - Acceptance 2: concurrent acquire contention

    @Test("Concurrent acquire on same episode: one wins, second raises LeaseHeld")
    func concurrentAcquireProducesLeaseHeld() async throws {
        let store = try await makeTestStore()
        let episodeId = "ep-conflict"
        try await store.insertJob(makeAnalysisJob(jobId: "j-conflict", episodeId: episodeId, workKey: "wk-conflict"))

        let epoch = try await store.fetchSchedulerEpoch() ?? 0
        let now: Double = 2_000_000

        _ = try await store.acquireEpisodeLease(
            episodeId: episodeId,
            ownerWorkerId: "worker-A",
            generationID: UUID().uuidString,
            schedulerEpoch: epoch,
            now: now,
            ttlSeconds: 60
        )

        do {
            _ = try await store.acquireEpisodeLease(
                episodeId: episodeId,
                ownerWorkerId: "worker-B",
                generationID: UUID().uuidString,
                schedulerEpoch: epoch,
                now: now + 1,
                ttlSeconds: 60
            )
            Issue.record("Expected LeaseError.leaseHeld")
        } catch LeaseError.leaseHeld(let ep) {
            #expect(ep == episodeId)
        }

        let fetched = try await store.fetchJob(byId: "j-conflict")
        #expect(fetched?.leaseOwner == "worker-A")
    }

    @Test("No analysis_jobs row for episode: acquire throws noJobForEpisode")
    func acquireWithoutRowThrowsNoJob() async throws {
        let store = try await makeTestStore()
        let epoch = try await store.fetchSchedulerEpoch() ?? 0
        do {
            _ = try await store.acquireEpisodeLease(
                episodeId: "nonexistent",
                ownerWorkerId: "worker-1",
                generationID: UUID().uuidString,
                schedulerEpoch: epoch,
                now: 0,
                ttlSeconds: 30
            )
            Issue.record("Expected LeaseError.noJobForEpisode")
        } catch LeaseError.noJobForEpisode(let ep) {
            #expect(ep == "nonexistent")
        }
    }

    // MARK: - Acceptance 3: renew with stale epoch

    @Test("Renew with stale epoch: throws StaleEpoch; lease untouched")
    func renewWithStaleEpochThrows() async throws {
        let store = try await makeTestStore()
        let episodeId = "ep-renew"
        try await store.insertJob(makeAnalysisJob(jobId: "j-renew", episodeId: episodeId, workKey: "wk-renew"))

        let gen = UUID()
        let originalEpoch = try await store.fetchSchedulerEpoch() ?? 0
        let now: Double = 3_000_000
        let descriptor = try await store.acquireEpisodeLease(
            episodeId: episodeId,
            ownerWorkerId: "worker-1",
            generationID: gen.uuidString,
            schedulerEpoch: originalEpoch,
            now: now,
            ttlSeconds: 30
        )
        let originalExpiry = descriptor.expiresAt

        // Scheduler bumps the epoch - simulates a cross-episode
        // promotion/demotion.
        let newEpoch = try await store.incrementSchedulerEpoch()
        #expect(newEpoch == originalEpoch + 1)

        // Worker tries to renew with its (now stale) epoch.
        do {
            try await store.renewEpisodeLease(
                episodeId: episodeId,
                generationID: gen.uuidString,
                schedulerEpoch: originalEpoch,
                newExpiresAt: now + 300,
                now: now + 5
            )
            Issue.record("Expected LeaseError.staleEpoch")
        } catch LeaseError.staleEpoch(let expected, let actual) {
            #expect(expected == originalEpoch)
            #expect(actual == newEpoch)
        }

        // Lease untouched.
        let fetched = try await store.fetchJob(byId: "j-renew")
        #expect(fetched?.leaseExpiresAt == originalExpiry)
        #expect(fetched?.schedulerEpoch == originalEpoch)
    }

    @Test("Renew with matching epoch + gen: extends expiry")
    func renewWithMatchingEpochSucceeds() async throws {
        let store = try await makeTestStore()
        let episodeId = "ep-renew-ok"
        try await store.insertJob(makeAnalysisJob(jobId: "j-renew-ok", episodeId: episodeId, workKey: "wk-renew-ok"))
        let gen = UUID()
        let epoch = try await store.fetchSchedulerEpoch() ?? 0
        _ = try await store.acquireEpisodeLease(
            episodeId: episodeId,
            ownerWorkerId: "worker-1",
            generationID: gen.uuidString,
            schedulerEpoch: epoch,
            now: 4_000_000,
            ttlSeconds: 30
        )

        try await store.renewEpisodeLease(
            episodeId: episodeId,
            generationID: gen.uuidString,
            schedulerEpoch: epoch,
            newExpiresAt: 4_000_500,
            now: 4_000_010
        )
        let fetched = try await store.fetchJob(byId: "j-renew-ok")
        #expect(fetched?.leaseExpiresAt == 4_000_500)
    }

    @Test("Renew with mismatched generation: throws generationMismatch")
    func renewWithWrongGenerationThrows() async throws {
        let store = try await makeTestStore()
        let episodeId = "ep-wrong-gen"
        try await store.insertJob(makeAnalysisJob(jobId: "j-wg", episodeId: episodeId, workKey: "wk-wg"))
        let gen = UUID()
        let epoch = try await store.fetchSchedulerEpoch() ?? 0
        _ = try await store.acquireEpisodeLease(
            episodeId: episodeId,
            ownerWorkerId: "worker-1",
            generationID: gen.uuidString,
            schedulerEpoch: epoch,
            now: 5_000_000,
            ttlSeconds: 30
        )

        let wrongGen = UUID()
        do {
            try await store.renewEpisodeLease(
                episodeId: episodeId,
                generationID: wrongGen.uuidString,
                schedulerEpoch: epoch,
                newExpiresAt: 5_000_999,
                now: 5_000_010
            )
            Issue.record("Expected LeaseError.generationMismatch")
        } catch LeaseError.generationMismatch(let ep) {
            #expect(ep == episodeId)
        }
    }

    @Test("Renew after lease expiry: throws generationMismatch; zombie cannot resurrect")
    func renewAfterExpiryThrows() async throws {
        let store = try await makeTestStore()
        let episodeId = "ep-expired"
        try await store.insertJob(makeAnalysisJob(jobId: "j-exp", episodeId: episodeId, workKey: "wk-exp"))
        let gen = UUID()
        let epoch = try await store.fetchSchedulerEpoch() ?? 0
        let acquireAt: Double = 7_000_000
        let ttl: Double = 30
        let descriptor = try await store.acquireEpisodeLease(
            episodeId: episodeId,
            ownerWorkerId: "worker-zombie",
            generationID: gen.uuidString,
            schedulerEpoch: epoch,
            now: acquireAt,
            ttlSeconds: ttl
        )
        // Worker goes to sleep past its TTL, then wakes up and tries to
        // extend its own already-expired lease. The renew must fail
        // loudly so another acquirer can take the row.
        let afterExpiry = descriptor.expiresAt + 1
        do {
            try await store.renewEpisodeLease(
                episodeId: episodeId,
                generationID: gen.uuidString,
                schedulerEpoch: epoch,
                newExpiresAt: afterExpiry + 300,
                now: afterExpiry
            )
            Issue.record("Expected LeaseError.generationMismatch for expired lease")
        } catch LeaseError.generationMismatch(let ep) {
            #expect(ep == episodeId)
        }
        // Expiry timestamp untouched so another acquirer sees it as free.
        let fetched = try await store.fetchJob(byId: "j-exp")
        #expect(fetched?.leaseExpiresAt == descriptor.expiresAt)
    }

    @Test("Renew at exact expiry boundary succeeds: leaseExpiresAt == now is live")
    func renewAtExpiryBoundarySucceeds() async throws {
        // Boundary control for the `leaseExpiresAt >= ?` predicate in
        // `renewEpisodeLease`. A mutation from `>=` to `>` would pass
        // both `renewAfterExpiryThrows` (strict expiry, `now > expiry`)
        // and the generation-mismatch test (different gen entirely),
        // but this case — where the owner renews exactly at the TTL
        // cliff — distinguishes the two and locks in the inclusive
        // boundary.
        let store = try await makeTestStore()
        let episodeId = "ep-boundary"
        try await store.insertJob(
            makeAnalysisJob(jobId: "j-boundary", episodeId: episodeId, workKey: "wk-boundary")
        )
        let gen = UUID()
        let epoch = try await store.fetchSchedulerEpoch() ?? 0
        let acquireAt: Double = 9_000_000
        let ttl: Double = 30
        let descriptor = try await store.acquireEpisodeLease(
            episodeId: episodeId,
            ownerWorkerId: "worker-boundary",
            generationID: gen.uuidString,
            schedulerEpoch: epoch,
            now: acquireAt,
            ttlSeconds: ttl
        )
        // Renew exactly AT the expiry instant. With `>=`, this succeeds;
        // with `>`, this would spuriously throw.
        let atExpiry = descriptor.expiresAt
        try await store.renewEpisodeLease(
            episodeId: episodeId,
            generationID: gen.uuidString,
            schedulerEpoch: epoch,
            newExpiresAt: atExpiry + 60,
            now: atExpiry
        )
        let fetched = try await store.fetchJob(byId: "j-boundary")
        #expect(fetched?.leaseExpiresAt == atExpiry + 60)
    }

    @Test("Fresh acquirer reclaims row after zombie renew failed: proves repair path")
    func freshAcquirerReclaimsAfterZombieFailed() async throws {
        let store = try await makeTestStore()
        let episodeId = "ep-repair"
        try await store.insertJob(makeAnalysisJob(jobId: "j-repair", episodeId: episodeId, workKey: "wk-repair"))
        let zombieGen = UUID()
        let epoch = try await store.fetchSchedulerEpoch() ?? 0
        let descriptor = try await store.acquireEpisodeLease(
            episodeId: episodeId,
            ownerWorkerId: "worker-zombie",
            generationID: zombieGen.uuidString,
            schedulerEpoch: epoch,
            now: 8_000_000,
            ttlSeconds: 30
        )
        // Phase 1: zombie wakes past TTL, attempt to renew → must throw.
        let afterExpiry = descriptor.expiresAt + 1
        do {
            try await store.renewEpisodeLease(
                episodeId: episodeId,
                generationID: zombieGen.uuidString,
                schedulerEpoch: epoch,
                newExpiresAt: afterExpiry + 300,
                now: afterExpiry
            )
            Issue.record("Expected zombie renew to throw")
        } catch LeaseError.generationMismatch {
            // expected
        }

        // Phase 2: fresh worker takes over with a new generationID. The
        // row's `leaseExpiresAt` is still the stale zombie expiry, which
        // is < now — so `acquireEpisodeLease`'s eligibility predicate
        // (`leaseOwner IS NULL OR leaseExpiresAt < ?`) must treat the
        // slot as free and hand the lease to the new worker.
        let freshGen = UUID()
        let reclaimed = try await store.acquireEpisodeLease(
            episodeId: episodeId,
            ownerWorkerId: "worker-fresh",
            generationID: freshGen.uuidString,
            schedulerEpoch: epoch,
            now: afterExpiry,
            ttlSeconds: 30
        )
        #expect(reclaimed.ownerWorkerId == "worker-fresh")
        #expect(reclaimed.generationID == freshGen.uuidString)
        #expect(reclaimed.expiresAt == afterExpiry + 30)

        // Persisted row reflects the new owner, not the zombie.
        let fetched = try await store.fetchJob(byId: "j-repair")
        #expect(fetched?.leaseOwner == "worker-fresh")
        #expect(fetched?.generationID == freshGen.uuidString)
    }

    // MARK: - Acceptance 4: idempotent finalize

    @Test("Duplicate releaseLease(finalized) is a no-op; journal has exactly one entry")
    func duplicateFinalizedRelease_isIdempotent() async throws {
        let store = try await makeTestStore()
        let episodeId = "ep-dup-fin"
        try await store.insertJob(makeAnalysisJob(jobId: "j-dup", episodeId: episodeId, workKey: "wk-dup-fin"))
        let gen = UUID()
        let epoch = try await store.fetchSchedulerEpoch() ?? 0
        _ = try await store.acquireEpisodeLease(
            episodeId: episodeId,
            ownerWorkerId: "worker-1",
            generationID: gen.uuidString,
            schedulerEpoch: epoch,
            now: 6_000_000,
            ttlSeconds: 30
        )

        try await store.releaseEpisodeLease(
            episodeId: episodeId,
            generationID: gen.uuidString,
            schedulerEpoch: epoch,
            eventType: .finalized,
            cause: nil,
            now: 6_000_100
        )
        // Second call against the same {episode, generation, finalized}
        // must succeed without error and without appending another row.
        try await store.releaseEpisodeLease(
            episodeId: episodeId,
            generationID: gen.uuidString,
            schedulerEpoch: epoch,
            eventType: .finalized,
            cause: nil,
            now: 6_000_200
        )

        let entries = try await store.fetchWorkJournalEntries(
            episodeId: episodeId,
            generationID: gen.uuidString
        )
        let finalizedCount = entries.filter { $0.eventType == .finalized }.count
        #expect(finalizedCount == 1)

        // Lease slot cleared.
        let fetched = try await store.fetchJob(byId: "j-dup")
        #expect(fetched?.leaseOwner == nil)
        #expect(fetched?.leaseExpiresAt == nil)
    }

    @Test("release with InternalMissCause persists cause in journal (emission channel for playhead-1nl6)")
    func releasePersistsCause() async throws {
        let store = try await makeTestStore()
        let episodeId = "ep-cause"
        try await store.insertJob(makeAnalysisJob(jobId: "j-cause", episodeId: episodeId, workKey: "wk-cause"))
        let gen = UUID()
        let epoch = try await store.fetchSchedulerEpoch() ?? 0
        _ = try await store.acquireEpisodeLease(
            episodeId: episodeId,
            ownerWorkerId: "worker-1",
            generationID: gen.uuidString,
            schedulerEpoch: epoch,
            now: 7_000_000,
            ttlSeconds: 30
        )
        try await store.releaseEpisodeLease(
            episodeId: episodeId,
            generationID: gen.uuidString,
            schedulerEpoch: epoch,
            eventType: .preempted,
            cause: .taskExpired,
            now: 7_000_100
        )
        let entries = try await store.fetchWorkJournalEntries(
            episodeId: episodeId,
            generationID: gen.uuidString
        )
        let preempted = entries.first(where: { $0.eventType == .preempted })
        #expect(preempted?.cause == .taskExpired)
    }

    // MARK: - Acceptance 5: orphan requeue w/ checkpoint

    @Test("Orphan w/ checkpointed journal: requeue resets gen, bumps epoch, lane preserved")
    func orphanRequeueAfterCheckpoint() async throws {
        let store = try await makeTestStore()

        let episodeId = "ep-orphan-ckpt"
        // Seed a Soon-lane row (priority=10) with an expired lease +
        // checkpointed journal entry - the most common orphan shape.
        let originalGen = UUID()
        let originalEpoch = try await store.fetchSchedulerEpoch() ?? 0
        let now: Double = 8_000_000
        let expiredAt = now - 15  // 15 s ago

        var job = makeAnalysisJob(
            jobId: "j-orphan-ckpt",
            episodeId: episodeId,
            workKey: "wk-orphan-ckpt",
            priority: 10,
            attemptCount: 3,
            leaseOwner: "worker-stale",
            leaseExpiresAt: expiredAt
        )
        job = spliceLeaseIdentity(
            job: job,
            generationID: originalGen.uuidString,
            schedulerEpoch: originalEpoch
        )
        try await store.insertJob(job)

        // Seed a checkpointed journal entry for this generation.
        let ckpt = WorkJournalEntry(
            id: UUID().uuidString,
            episodeId: episodeId,
            generationID: originalGen,
            schedulerEpoch: originalEpoch,
            timestamp: now - 20,
            eventType: .checkpointed,
            cause: nil,
            metadata: "{}",
            artifactClass: .scratch
        )
        try await store.appendWorkJournalEntry(ckpt)

        let rebuilt = try await recoverOrphans(store: store, now: now, graceSeconds: 10)
        #expect(rebuilt.count == 1)

        let recovered = try await store.fetchJob(byId: "j-orphan-ckpt")
        #expect(recovered?.leaseOwner == nil)
        #expect(recovered?.leaseExpiresAt == nil)
        #expect(recovered?.attemptCount == 0)
        #expect(recovered?.generationID != originalGen.uuidString)
        #expect(recovered?.generationID.isEmpty == false)
        #expect((recovered?.schedulerEpoch ?? 0) > originalEpoch)
        #expect(recovered?.priority == 10, "Soon lane preserved")

        // Journal row for the original generation remains.
        let originalEntries = try await store.fetchWorkJournalEntries(
            episodeId: episodeId,
            generationID: originalGen.uuidString
        )
        #expect(!originalEntries.isEmpty)
    }

    // MARK: - Acceptance 6: Now-lane demotion

    @Test("Now-lane orphan stale >60 s demotes to Soon (priority 10)")
    func nowLaneOrphanDemotesToSoon() async throws {
        let store = try await makeTestStore()

        let episodeId = "ep-now-demote"
        let originalGen = UUID()
        let originalEpoch = try await store.fetchSchedulerEpoch() ?? 0
        let now: Double = 9_000_000
        let expiredAt = now - 120 // 120 s > 60 s threshold

        var job = makeAnalysisJob(
            jobId: "j-now-demote",
            episodeId: episodeId,
            workKey: "wk-now-demote",
            priority: 20, // Now lane sentinel (> soonLaneOrphanPriority=10)
            leaseOwner: "worker-now",
            leaseExpiresAt: expiredAt
        )
        job = spliceLeaseIdentity(
            job: job,
            generationID: originalGen.uuidString,
            schedulerEpoch: originalEpoch
        )
        try await store.insertJob(job)

        // No checkpoint - the `acquired` entry was the only one.
        let acq = WorkJournalEntry(
            id: UUID().uuidString,
            episodeId: episodeId,
            generationID: originalGen,
            schedulerEpoch: originalEpoch,
            timestamp: now - 140,
            eventType: .acquired,
            cause: nil,
            metadata: "{}",
            artifactClass: .scratch
        )
        try await store.appendWorkJournalEntry(acq)

        _ = try await recoverOrphans(store: store, now: now, graceSeconds: 10)
        let recovered = try await store.fetchJob(byId: "j-now-demote")
        #expect(recovered?.priority == AnalysisCoordinator.soonLaneOrphanPriority)
    }

    @Test("Now-lane orphan stale <=60 s stays in Now lane")
    func nowLaneOrphanWithinThresholdNotDemoted() async throws {
        let store = try await makeTestStore()

        let episodeId = "ep-now-keep"
        let originalGen = UUID()
        let originalEpoch = try await store.fetchSchedulerEpoch() ?? 0
        let now: Double = 10_000_000
        let expiredAt = now - 45 // less than 60 s stale

        var job = makeAnalysisJob(
            jobId: "j-now-keep",
            episodeId: episodeId,
            workKey: "wk-now-keep",
            priority: 20,
            leaseOwner: "worker-now",
            leaseExpiresAt: expiredAt
        )
        job = spliceLeaseIdentity(
            job: job,
            generationID: originalGen.uuidString,
            schedulerEpoch: originalEpoch
        )
        try await store.insertJob(job)
        try await store.appendWorkJournalEntry(WorkJournalEntry(
            id: UUID().uuidString,
            episodeId: episodeId,
            generationID: originalGen,
            schedulerEpoch: originalEpoch,
            timestamp: now - 50,
            eventType: .checkpointed,
            cause: nil,
            metadata: "{}",
            artifactClass: .scratch
        ))

        _ = try await recoverOrphans(store: store, now: now, graceSeconds: 10)
        let recovered = try await store.fetchJob(byId: "j-now-keep")
        #expect(recovered?.priority == 20, "Below demotion threshold should stay in Now lane")
    }

    // MARK: - Terminal event orphans don't requeue

    @Test("Orphan with terminal (finalized) last event: lease cleared, no requeue")
    func orphanWithTerminalEventCleared() async throws {
        let store = try await makeTestStore()

        let episodeId = "ep-terminal"
        let originalGen = UUID()
        let originalEpoch = try await store.fetchSchedulerEpoch() ?? 0
        let now: Double = 11_000_000
        let expiredAt = now - 20

        var job = makeAnalysisJob(
            jobId: "j-terminal",
            episodeId: episodeId,
            workKey: "wk-term",
            priority: 10,
            attemptCount: 4,
            leaseOwner: "worker-zombie",
            leaseExpiresAt: expiredAt
        )
        job = spliceLeaseIdentity(
            job: job,
            generationID: originalGen.uuidString,
            schedulerEpoch: originalEpoch
        )
        try await store.insertJob(job)
        try await store.appendWorkJournalEntry(WorkJournalEntry(
            id: UUID().uuidString,
            episodeId: episodeId,
            generationID: originalGen,
            schedulerEpoch: originalEpoch,
            timestamp: now - 30,
            eventType: .finalized,
            cause: nil,
            metadata: "{}",
            artifactClass: .scratch
        ))

        let rebuilt = try await recoverOrphans(store: store, now: now, graceSeconds: 10)
        #expect(rebuilt.isEmpty, "Terminal orphans should not be requeued")

        let fetched = try await store.fetchJob(byId: "j-terminal")
        #expect(fetched?.leaseOwner == nil)
        #expect(fetched?.leaseExpiresAt == nil)
        #expect(fetched?.attemptCount == 4, "Terminal clear must not reset attemptCount")
        #expect(fetched?.generationID == originalGen.uuidString, "Generation preserved (no requeue)")
    }

    // MARK: - Acceptance 7: crash consistency

    @Test("Crash mid-scheduling-pass: rollback leaves DB consistent (no partial work_journal)")
    func crashMidSchedulingPassRollsBack() async throws {
        let store = try await makeTestStore()
        let episodeId = "ep-rollback"
        try await store.insertJob(makeAnalysisJob(jobId: "j-rb", episodeId: episodeId, workKey: "wk-rb"))

        let epochBefore = try await store.fetchSchedulerEpoch()
        let gen = UUID()

        // `simulateCrashInSchedulingPassForTesting` bumps the scheduler
        // epoch, appends a journal row, and throws - all inside the
        // outer runSchedulingPass envelope. The throw must roll back
        // every step.
        await #expect(throws: AnalysisStore.CrashRollbackTestError.self) {
            try await store.simulateCrashInSchedulingPassForTesting(
                episodeId: episodeId,
                generationID: gen,
                timestamp: 0
            )
        }

        // Both the epoch and the journal should be untouched.
        let epochAfter = try await store.fetchSchedulerEpoch()
        #expect(epochBefore == epochAfter)
        let entries = try await store.fetchWorkJournalEntries(
            episodeId: episodeId,
            generationID: gen.uuidString
        )
        #expect(entries.isEmpty)
    }

    // MARK: - playhead-5uvz.1: acquireLeaseWithJournal (Gap-1)

    @Test("acquireLeaseWithJournal: success appends one .acquired row and stamps row identity")
    func acquireLeaseWithJournalSuccessJournalAndIdentity() async throws {
        let store = try await makeTestStore()
        let job = makeAnalysisJob(jobId: "j-acq-1", episodeId: "ep-acq-1", workKey: "wk-acq-1")
        try await store.insertJob(job)

        let now = 1_000.0
        let acquired = try await store.acquireLeaseWithJournal(
            jobId: "j-acq-1",
            episodeId: "ep-acq-1",
            owner: "preAnalysis",
            expiresAt: now + 300,
            now: now
        )
        #expect(acquired == true)

        // Row carries the freshly-minted generationID + epoch.
        let fetched = try await store.fetchJob(byId: "j-acq-1")
        #expect(fetched?.leaseOwner == "preAnalysis")
        #expect(fetched?.leaseExpiresAt == now + 300)
        #expect(fetched?.state == "running")
        #expect(fetched?.generationID != "", "generationID must be stamped on acquire")
        // playhead-uzdq seeds _meta.scheduler_epoch='1' on migration, so
        // the first acquire stamps epoch=1 (not 0).
        #expect(fetched?.schedulerEpoch == 1, "Migrated epoch on first acquire")

        // The journal row's {episodeId, generationID} matches the row.
        let lastEvent = try await store.fetchLastWorkJournalEntry(
            episodeId: "ep-acq-1",
            generationID: fetched?.generationID ?? ""
        )
        #expect(lastEvent?.eventType == .acquired)
        #expect(lastEvent?.timestamp == now)
    }

    @Test("acquireLeaseWithJournal: contention returns false and writes no journal row")
    func acquireLeaseWithJournalContentionNoJournal() async throws {
        let store = try await makeTestStore()
        let job = makeAnalysisJob(jobId: "j-acq-2", episodeId: "ep-acq-2", workKey: "wk-acq-2")
        try await store.insertJob(job)

        let now = 2_000.0
        let first = try await store.acquireLeaseWithJournal(
            jobId: "j-acq-2",
            episodeId: "ep-acq-2",
            owner: "preAnalysis",
            expiresAt: now + 300,
            now: now
        )
        #expect(first == true)

        let firstFetched = try await store.fetchJob(byId: "j-acq-2")
        let firstGen = firstFetched?.generationID ?? ""

        // Second worker contends — lease still live, must return false.
        let second = try await store.acquireLeaseWithJournal(
            jobId: "j-acq-2",
            episodeId: "ep-acq-2",
            owner: "preAnalysis-other",
            expiresAt: now + 600,
            now: now + 1
        )
        #expect(second == false, "Live lease must reject second acquire")

        // Row identity must be unchanged (no second-owner clobber).
        let after = try await store.fetchJob(byId: "j-acq-2")
        #expect(after?.leaseOwner == "preAnalysis")
        #expect(after?.generationID == firstGen, "generationID must not rotate on contention")

        // Journal must contain exactly one .acquired row for this {episode, gen}.
        let entries = try await store.fetchWorkJournalEntries(
            episodeId: "ep-acq-2",
            generationID: firstGen
        )
        #expect(entries.count == 1)
        #expect(entries.first?.eventType == .acquired)
    }

    @Test("acquireLeaseWithJournal: expired prior lease still produces journal row on takeover")
    func acquireLeaseWithJournalExpiredTakeoverJournals() async throws {
        let store = try await makeTestStore()
        let job = makeAnalysisJob(jobId: "j-acq-3", episodeId: "ep-acq-3", workKey: "wk-acq-3")
        try await store.insertJob(job)

        let t0 = 3_000.0
        _ = try await store.acquireLeaseWithJournal(
            jobId: "j-acq-3",
            episodeId: "ep-acq-3",
            owner: "preAnalysis",
            expiresAt: t0 + 60,  // short TTL
            now: t0
        )

        // Move clock past expiry, second worker takes over.
        let t1 = t0 + 120
        let takeover = try await store.acquireLeaseWithJournal(
            jobId: "j-acq-3",
            episodeId: "ep-acq-3",
            owner: "preAnalysis-takeover",
            expiresAt: t1 + 300,
            now: t1
        )
        #expect(takeover == true)

        let fetched = try await store.fetchJob(byId: "j-acq-3")
        #expect(fetched?.leaseOwner == "preAnalysis-takeover")
        let newGen = fetched?.generationID ?? ""

        // Two journal entries exist for the episode (one per generation),
        // each carrying the .acquired event type. The second generation's
        // last event matches the new row identity.
        let lastEvent = try await store.fetchLastWorkJournalEntry(
            episodeId: "ep-acq-3",
            generationID: newGen
        )
        #expect(lastEvent?.eventType == .acquired)
        #expect(lastEvent?.timestamp == t1)
    }

    @Test("Cold-launch journal-aware path: empty-journal post-acquire orphan routes to resume branch")
    func coldLaunchEmptyJournalRoutesToResume() async throws {
        // Simulates the very first run after 5uvz.1 ships: a job that
        // was acquired BEFORE the new wiring (no journal trail) crashes
        // mid-flight. recoverOrphans must NOT skip it — the resume
        // branch fires when fetchLastWorkJournalEntry returns nil.
        let store = try await makeTestStore()
        let job = makeAnalysisJob(
            jobId: "j-empty",
            episodeId: "ep-empty",
            workKey: "wk-empty",
            state: "running",
            leaseOwner: "preAnalysis",
            leaseExpiresAt: 1_000  // expired
        )
        try await store.insertJob(job)

        let rebuilt = try await recoverOrphans(store: store, now: 2_000, graceSeconds: 10)
        #expect(rebuilt == ["j-empty"], "Empty-journal orphan must requeue, not be silently dropped")

        // Row was rotated: fresh generationID, fresh epoch, lease cleared.
        let after = try await store.fetchJob(byId: "j-empty")
        #expect(after?.leaseOwner == nil)
        #expect(after?.leaseExpiresAt == nil)
        #expect(after?.generationID != "")
        #expect((after?.schedulerEpoch ?? 0) > 0, "Epoch must bump on requeue")
    }

    @Test("Acquire-then-crash flow: acquired row is visible to recoverOrphans by generation join")
    func acquireThenCrashFlow() async throws {
        // End-to-end flow in one test: 5uvz.1 acquire writes journal,
        // then we simulate crash by manually expiring the lease, then
        // 5uvz.2 recoverOrphans reads the .acquired row and routes the
        // orphan correctly.
        let store = try await makeTestStore()
        let job = makeAnalysisJob(
            jobId: "j-crash",
            episodeId: "ep-crash",
            workKey: "wk-crash"
        )
        try await store.insertJob(job)

        let acquireTime = 1_000.0
        _ = try await store.acquireLeaseWithJournal(
            jobId: "j-crash",
            episodeId: "ep-crash",
            owner: "preAnalysis",
            expiresAt: acquireTime + 30,
            now: acquireTime
        )

        // Snapshot the row's generationID (the one stamped at acquire).
        let mid = try await store.fetchJob(byId: "j-crash")
        let crashedGen = mid?.generationID ?? ""
        #expect(crashedGen != "")

        // Now simulate cold-launch reaper running well past expiry.
        let coldLaunch = acquireTime + 120
        let rebuilt = try await recoverOrphans(
            store: store,
            now: coldLaunch,
            graceSeconds: 10
        )
        #expect(rebuilt == ["j-crash"], "Acquired-then-crashed orphan must requeue via journal-aware path")

        // The row's generationID rotated; lease cleared; epoch bumped.
        let after = try await store.fetchJob(byId: "j-crash")
        #expect(after?.generationID != crashedGen, "generationID must rotate on requeue")
        #expect(after?.leaseOwner == nil)
        #expect((after?.schedulerEpoch ?? 0) > 0)

        // The original .acquired row is still readable by the original
        // generation (journal append-only invariant).
        let originalGenLast = try await store.fetchLastWorkJournalEntry(
            episodeId: "ep-crash",
            generationID: crashedGen
        )
        #expect(originalGenLast?.eventType == .acquired)
    }

    // MARK: - Helpers

    /// Replays `AnalysisCoordinator.recoverOrphans` against the store's
    /// public primitives. Kept in the test file (instead of going
    /// through the coordinator) so the test does not have to
    /// construct the coordinator's 6+ collaborator services. This
    /// policy is also exercised indirectly by the coordinator's own
    /// integration tests.
    @discardableResult
    private func recoverOrphans(
        store: AnalysisStore,
        now: Double,
        graceSeconds: Double
    ) async throws -> [String] {
        let stale = try await store.fetchEpisodesWithExpiredLeases(
            now: now,
            graceSeconds: graceSeconds
        )
        guard !stale.isEmpty else { return [] }

        let currentEpoch = try await store.fetchSchedulerEpoch() ?? 0
        var rebuilt: [String] = []
        for job in stale {
            let lastEvent = try await store.fetchLastWorkJournalEntry(
                episodeId: job.episodeId,
                generationID: job.generationID
            )
            // Production policy (AnalysisCoordinator.recoverOrphans):
            // a journal row at a future epoch indicates corruption — the
            // lease is held but the journal disagrees with our scheduler
            // generation. Skipping recovery preserves whatever terminal
            // state exists rather than redoing work or requeueing
            // something that may already be done.
            if let last = lastEvent, last.schedulerEpoch > currentEpoch {
                continue
            }
            let decisionEvent = lastEvent?.eventType

            switch decisionEvent {
            case .finalized, .failed:
                try await store.clearOrphanedLeaseNoRequeue(
                    jobId: job.jobId,
                    now: now
                )
            case .checkpointed, .acquired, .preempted, .none:
                let newEpoch = try await store.incrementSchedulerEpoch()
                let newGeneration = UUID().uuidString
                let staleSeconds = max(0.0, now - (job.leaseExpiresAt ?? now))
                let newPriority = AnalysisCoordinator.laneAfterOrphan(
                    currentPriority: job.priority,
                    staleSeconds: staleSeconds
                )
                try await store.requeueOrphanedLease(
                    jobId: job.jobId,
                    newGenerationID: newGeneration,
                    newSchedulerEpoch: newEpoch,
                    newPriority: newPriority,
                    now: now
                )
                rebuilt.append(job.jobId)
            }
        }
        return rebuilt
    }

    /// Workaround: `AnalysisJob` fields are `let`, so to splice the
    /// lease identity onto a factory-built job we rebuild it.
    private func spliceLeaseIdentity(
        job: AnalysisJob,
        generationID: String,
        schedulerEpoch: Int
    ) -> AnalysisJob {
        AnalysisJob(
            jobId: job.jobId,
            jobType: job.jobType,
            episodeId: job.episodeId,
            podcastId: job.podcastId,
            analysisAssetId: job.analysisAssetId,
            workKey: job.workKey,
            sourceFingerprint: job.sourceFingerprint,
            downloadId: job.downloadId,
            priority: job.priority,
            desiredCoverageSec: job.desiredCoverageSec,
            featureCoverageSec: job.featureCoverageSec,
            transcriptCoverageSec: job.transcriptCoverageSec,
            cueCoverageSec: job.cueCoverageSec,
            state: job.state,
            attemptCount: job.attemptCount,
            nextEligibleAt: job.nextEligibleAt,
            leaseOwner: job.leaseOwner,
            leaseExpiresAt: job.leaseExpiresAt,
            lastErrorCode: job.lastErrorCode,
            createdAt: job.createdAt,
            updatedAt: job.updatedAt,
            generationID: generationID,
            schedulerEpoch: schedulerEpoch
        )
    }

    private func countColumn(in directory: URL, table: String, column: String) throws -> Int {
        let dbURL = directory.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        guard sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READONLY, nil) == SQLITE_OK else {
            throw NSError(domain: "CountCol", code: 1)
        }
        defer { sqlite3_close_v2(db) }
        var stmt: OpaquePointer?
        guard sqlite3_prepare_v2(db, "PRAGMA table_info(\(table))", -1, &stmt, nil) == SQLITE_OK else {
            throw NSError(domain: "CountCol", code: 2)
        }
        defer { sqlite3_finalize(stmt) }
        var count = 0
        while sqlite3_step(stmt) == SQLITE_ROW {
            if let c = sqlite3_column_text(stmt, 1), String(cString: c) == column {
                count += 1
            }
        }
        return count
    }
}
