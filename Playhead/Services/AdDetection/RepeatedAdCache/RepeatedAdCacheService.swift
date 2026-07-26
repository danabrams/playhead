// RepeatedAdCacheService.swift
// playhead-43ed (B3): local, show-scoped recurrence index for confirmed
// ad-span fingerprints. A match is telemetry/diagnostic evidence only; the
// current episode's classifier remains authoritative for its boundaries and
// confidence.
//
// Lifecycle:
//   `store(...)` after a consumed skip or explicit confirmation ↷
//   `lookup(...)` alongside classification on the next episode ↷
//   `recordOutcome(hit:)` keeps a rolling 14-day hit-rate window ↷
//   below the configured floor → cache auto-disables itself
//
// Concurrency:
//   The service is an `actor` so writes serialise. The clock is
//   injected (`@Sendable () -> Date`) for deterministic tests.
//
// Storage:
//   Backed by ``RepeatedAdCacheStorage``. Two impls exist: an in-memory
//   one for unit tests, and the AnalysisStore-backed production one
//   that persists across launches.

import Foundation
import OSLog

/// Self-disable reason — exposed so the Diagnostics surface can render
/// an honest "off because hit-rate too low" message instead of guessing.
enum RepeatedAdCacheDisableReason: Sendable, Equatable {
    case userKillSwitch
    case autoDisabledLowHitRate(observedHitRate: Double, samples: Int)
}

/// Errors returned to the runner by `lookup`. Distinguished from
/// `RepeatedAdCacheError` because callers want to differentiate "no hit"
/// (a normal outcome) from a real failure.
enum RepeatedAdCacheLookupOutcome: Sendable {
    case hit(RepeatedAdCacheEntry)
    case miss
    case skippedDisabled
}

actor RepeatedAdCacheService {

    // MARK: Stored state

    let config: RepeatedAdCacheConfig
    private let storage: any RepeatedAdCacheStorage
    private let clock: @Sendable () -> Date
    /// Side-effect hook invoked when the rolling-window guard auto-
    /// disables the cache. Defaults to a no-op; production wires it to
    /// persist the disabled state into UserDefaults so it survives an
    /// app launch (bead §4 implied persistence). The closure is
    /// `@Sendable` because the actor is allowed to escape it via Task.
    private let onAutoDisable: @Sendable (Double, Int) -> Void
    private let logger = Logger(subsystem: "com.playhead", category: "RepeatedAdCacheService")

    /// Live enable/disable. Defaults to `true`. Flipped to `false` by
    /// the user kill-switch (`setEnabled(false)`) OR by the auto-disable
    /// telemetry guard. Any flip → entries + outcome samples cleared.
    private var enabled: Bool

    /// Why the cache is currently disabled. `nil` while enabled.
    private var disableReason: RepeatedAdCacheDisableReason?
    /// Latest user/automatic intent, distinct from `enabled` while a
    /// re-enable is durably clearing any residue from an earlier failure.
    private var requestedEnabled: Bool

    /// Advances synchronously before every correction or cache-invalidating
    /// lifecycle transition touches storage. Operations that suspended on an
    /// older snapshot must fail closed when they re-enter the actor.
    private var evidenceGeneration: UInt64 = 0
    /// Advances only when enablement intent changes. Evidence corrections and
    /// explicit cache clears must invalidate stale lookups/writers, but they
    /// must not strand an already-committed kill-switch cleanup or suppress
    /// its durable auto-disable callback.
    private var enablementGeneration: UInt64 = 0
    /// Explicit clear keeps the feature enabled, so a separate fence prevents
    /// reentrant writers from repopulating between its two durable deletes.
    /// This is a count rather than a Boolean because two overlapping clears
    /// may suspend independently; the first completion must not reopen writes
    /// while the second still owns the destructive transaction.
    private var clearOperationsInFlight = 0
    private var clearInProgress: Bool {
        clearOperationsInFlight > 0
    }
    /// `recordOutcome` spans several storage awaits. Destructive transitions
    /// first block new samples, then wait for these pre-transition operations
    /// to finish before clearing the durable outcome table.
    private var outcomeOperationsInFlight = 0
    private var outcomeDrainWaiters: [CheckedContinuation<Void, Never>] = []
    /// Durable lifecycle cleanup is exclusive. A newer toggle records its
    /// generation before waiting, causing the older owner to stop at its next
    /// checkpoint; the newer owner then starts only after every older storage
    /// call has returned.
    private var lifecycleCleanupOwned = false
    private var lifecycleCleanupWaiters:
        [CheckedContinuation<Void, Never>] = []

#if DEBUG
    private var storeRevocationSnapshotBarrierForTesting:
        (@Sendable () async -> Void)?
    private var storePersistenceBarrierForTesting:
        (@Sendable () async -> Void)?
    private var lookupCandidateSnapshotBarrierForTesting:
        (@Sendable () async -> Void)?
    private var maintenanceClearBarrierForTesting:
        (@Sendable () async -> Void)?
    private var autoDisableRequestBarrierForTesting:
        (@Sendable () async -> Void)?
    private var lifecycleCleanupBarrierForTesting:
        (@Sendable () async -> Void)?
    private var outcomeDrainObserversForTesting:
        [CheckedContinuation<Void, Never>] = []
    private var lifecycleCleanupWaitObserversForTesting:
        [CheckedContinuation<Void, Never>] = []
#endif

    // MARK: Init

    init(
        config: RepeatedAdCacheConfig = .production,
        storage: any RepeatedAdCacheStorage,
        initiallyEnabled: Bool = true,
        clock: @Sendable @escaping () -> Date = { Date() },
        onAutoDisable: @Sendable @escaping (Double, Int) -> Void = { _, _ in }
    ) {
        self.config = config
        self.storage = storage
        self.clock = clock
        self.onAutoDisable = onAutoDisable
        self.enabled = initiallyEnabled
        self.requestedEnabled = initiallyEnabled
        self.disableReason = initiallyEnabled ? nil : .userKillSwitch
    }

    // MARK: Public surface

    func isEnabled() -> Bool { enabled }
    func currentDisableReason() -> RepeatedAdCacheDisableReason? { disableReason }

#if DEBUG
    func _setStoreRevocationSnapshotBarrierForTesting(
        _ barrier: (@Sendable () async -> Void)?
    ) {
        storeRevocationSnapshotBarrierForTesting = barrier
    }

    func _setStorePersistenceBarrierForTesting(
        _ barrier: (@Sendable () async -> Void)?
    ) {
        storePersistenceBarrierForTesting = barrier
    }

    func _setLookupCandidateSnapshotBarrierForTesting(
        _ barrier: (@Sendable () async -> Void)?
    ) {
        lookupCandidateSnapshotBarrierForTesting = barrier
    }

    func _setMaintenanceClearBarrierForTesting(
        _ barrier: (@Sendable () async -> Void)?
    ) {
        maintenanceClearBarrierForTesting = barrier
    }

    func _setAutoDisableRequestBarrierForTesting(
        _ barrier: (@Sendable () async -> Void)?
    ) {
        autoDisableRequestBarrierForTesting = barrier
    }

    func _setLifecycleCleanupBarrierForTesting(
        _ barrier: (@Sendable () async -> Void)?
    ) {
        lifecycleCleanupBarrierForTesting = barrier
    }

    func _waitUntilOutcomeDrainIsWaitingForTesting() async {
        guard outcomeDrainWaiters.isEmpty else { return }
        await withCheckedContinuation { continuation in
            outcomeDrainObserversForTesting.append(continuation)
        }
    }

    func _waitUntilLifecycleCleanupIsWaitingForTesting() async {
        guard lifecycleCleanupWaiters.isEmpty else { return }
        await withCheckedContinuation { continuation in
            lifecycleCleanupWaitObserversForTesting.append(continuation)
        }
    }
#endif

    /// Toggle the user kill switch. `false` clears entries + outcomes
    /// (bead §6: "disabling clears the cache"). `true` re-enables but
    /// does NOT repopulate — entries are populated lazily by future
    /// detections.
    func setEnabled(_ newValue: Bool) async {
        // A newer toggle can supersede a suspended re-enable after its intent
        // was recorded but before `enabled` flips to true. Treat that
        // requested/actual divergence as retryable.
        if newValue == requestedEnabled, newValue == enabled { return }
        requestedEnabled = newValue
        evidenceGeneration &+= 1
        enablementGeneration &+= 1
        let transitionGeneration = enablementGeneration
        if !newValue {
            // Close the hot path before waiting for an older cleanup owner.
            enabled = false
            disableReason = .userKillSwitch
        }
        await acquireLifecycleCleanup()
        defer { releaseLifecycleCleanup() }
        if !newValue {
            await waitForOutcomeOperationsToDrain()
            guard enablementGeneration == transitionGeneration,
                  requestedEnabled == newValue else {
                return
            }
            do {
                try await storage.clearEntries()
#if DEBUG
                if let barrier = lifecycleCleanupBarrierForTesting {
                    await barrier()
                }
#endif
                guard enablementGeneration == transitionGeneration,
                      requestedEnabled == newValue else {
                    return
                }
                try await storage.clearOutcomes()
                guard enablementGeneration == transitionGeneration,
                      requestedEnabled == newValue else {
                    return
                }
                logger.info(
                    "RepeatedAdCache: disabled by user kill-switch — entries + outcomes cleared"
                )
            } catch {
                guard enablementGeneration == transitionGeneration,
                      requestedEnabled == newValue else {
                    return
                }
                logger.error(
                    "RepeatedAdCache: kill-switch cleanup failed; cache remains disabled: \(String(describing: error), privacy: .public)"
                )
            }
        } else {
            // Retry both destructive clears before re-enabling. This prevents
            // rows left by an earlier I/O failure from being resurrected by a
            // kill-switch cycle.
            await waitForOutcomeOperationsToDrain()
            guard enablementGeneration == transitionGeneration,
                  requestedEnabled == newValue else {
                return
            }
            do {
                try await storage.clearEntries()
#if DEBUG
                if let barrier = maintenanceClearBarrierForTesting {
                    await barrier()
                }
#endif
                guard enablementGeneration == transitionGeneration,
                      requestedEnabled == newValue else {
                    return
                }
                try await storage.clearOutcomes()
            } catch {
                guard enablementGeneration == transitionGeneration,
                      requestedEnabled == newValue else {
                    return
                }
                enabled = false
                requestedEnabled = false
                disableReason = .userKillSwitch
                logger.error(
                    "RepeatedAdCache: re-enable cleanup failed; cache remains disabled: \(String(describing: error), privacy: .public)"
                )
                return
            }
            guard requestedEnabled,
                  enablementGeneration == transitionGeneration else {
                return
            }
            enabled = true
            disableReason = nil
            logger.info("RepeatedAdCache: re-enabled by user")
        }
    }

    /// Store an authoritative detection outcome. A classifier proposal alone
    /// is never sufficient: source/lifecycle must describe either a delayed
    /// consumed skip or an explicit positive user confirmation.
    /// No-op when the cache is disabled.
    /// No-op when `confidence < storeConfidenceThreshold` — bead §1
    /// requires only high-confidence hits to be cached.
    /// Returns `true` if the entry was actually persisted.
    @discardableResult
    func store(
        showId: String,
        fingerprint: RepeatedAdFingerprint,
        boundaryStart: Double,
        boundaryEnd: Double,
        confidence: Double,
        learningSource: CatalogLearningSource,
        learningLifecycle: CatalogLearningLifecycle,
        sourceAssetId: String,
        sourceWindowId: String,
        producerRevision: String = UUID().uuidString
    ) async throws -> Bool {
        guard enabled, !clearInProgress else { return false }
        guard confidence.isFinite,
              (0...1).contains(confidence),
              confidence >= config.storeConfidenceThreshold,
              boundaryStart.isFinite,
              boundaryEnd.isFinite,
              boundaryStart >= 0,
              boundaryEnd > boundaryStart else {
            return false
        }
        guard !fingerprint.isZero else {
            // Refuse to cache the all-zeros sentinel — it would collide
            // with any future zero-energy span and poison the cache.
            return false
        }
        guard let normalizedShowId =
                RecurrenceMaterialIdentity.canonicalIdentifier(showId),
              let normalizedSourceAssetId =
                RecurrenceMaterialIdentity.canonicalIdentifier(sourceAssetId),
              let normalizedSourceWindowId =
                RecurrenceMaterialIdentity.canonicalIdentifier(sourceWindowId),
              RecurrenceMaterialIdentity.canonicalIdentifier(
                  producerRevision
              ) != nil,
              learningSource.authoritativeLifecycle == learningLifecycle
        else {
            return false
        }
        let expectedEvidenceGeneration = evidenceGeneration

        let now = clock()
        guard now.timeIntervalSince1970.isFinite,
              now.timeIntervalSince1970 >= 0 else {
            return false
        }
        let revokedFingerprints = try await storage.fetchRevokedFingerprints(
            showId: normalizedShowId
        )
        guard evidenceGeneration == expectedEvidenceGeneration,
              !revokedFingerprints.contains(where: {
            $0.hammingDistance(to: fingerprint)
                <= config.hammingDistanceThreshold
        }) else {
            // A correction is terminal for this show's recurrence
            // neighborhood. Even a later explicit confirmation must not
            // silently rehabilitate it; a future product decision would need
            // an explicit tombstone-removal receipt.
            return false
        }
#if DEBUG
        if let barrier = storeRevocationSnapshotBarrierForTesting {
            await barrier()
        }
#endif
        guard evidenceGeneration == expectedEvidenceGeneration else {
            return false
        }
        let entry = RepeatedAdCacheEntry(
            showId: normalizedShowId,
            fingerprint: fingerprint,
            boundaryStart: boundaryStart,
            boundaryEnd: boundaryEnd,
            confidence: confidence,
            lastSeenAt: now,
            learningSource: learningSource,
            learningLifecycle: learningLifecycle,
            sourceAssetId: normalizedSourceAssetId,
            sourceWindowId: normalizedSourceWindowId,
            producerRevision: producerRevision
        )

        guard try await storage.upsertEnforcingCapacity(
            entry,
            perShowCap: config.perShowCap,
            globalCap: config.globalCap
        ) else {
            return false
        }
#if DEBUG
        if let barrier = storePersistenceBarrierForTesting {
            await barrier()
        }
#endif
        guard evidenceGeneration == expectedEvidenceGeneration,
              enabled,
              !clearInProgress else {
            // A correction entered while the durable write was suspended.
            // Compare-and-delete retracts only this exact persisted revision;
            // a newer UPSERT at the same key (even from the same source) wins.
            _ = try await storage.deleteIfUnchanged(entry)
            return false
        }

        return evidenceGeneration == expectedEvidenceGeneration
            && enabled
            && !clearInProgress
    }

    /// Retract one canceled recurrence write by its opaque producer revision.
    /// This is intentionally not a correction tombstone: a user seek cancels a
    /// pending consumed observation but does not veto future explicit evidence.
    @discardableResult
    func deleteIfProducerRevisionMatches(
        showId: String,
        fingerprint: RepeatedAdFingerprint,
        producerRevision: String
    ) async throws -> Bool {
        guard let normalizedShowId =
                RecurrenceMaterialIdentity.canonicalIdentifier(showId),
              !fingerprint.isZero,
              RecurrenceMaterialIdentity.canonicalIdentifier(
                  producerRevision
              ) != nil else {
            return false
        }
        // Only key + revision participate in storage comparison. The remaining
        // fields are inert placeholders required by the shared value type.
        return try await storage.deleteIfUnchanged(
            RepeatedAdCacheEntry(
                showId: normalizedShowId,
                fingerprint: fingerprint,
                boundaryStart: 0,
                boundaryEnd: 1,
                confidence: 0,
                lastSeenAt: Date(timeIntervalSince1970: 0),
                producerRevision: producerRevision
            )
        )
    }

    /// Look up a high-confidence cached entry that matches the given
    /// fingerprint within `hammingDistanceThreshold` AND belongs to
    /// the same show. Cache misses don't throw; they return
    /// `.miss`. `recordOutcome` is the caller's responsibility — the
    /// lookup itself only reads.
    func lookup(
        showId: String,
        fingerprint: RepeatedAdFingerprint
    ) async throws -> RepeatedAdCacheLookupOutcome {
        guard enabled, !clearInProgress else { return .skippedDisabled }
        guard !fingerprint.isZero else { return .miss }
        guard let normalizedShowId =
                RecurrenceMaterialIdentity.canonicalIdentifier(showId)
        else {
            return .miss
        }
        let expectedEvidenceGeneration = evidenceGeneration

        // Purge stale entries lazily so a hit can never return a
        // > 90-day entry.
        let lookupNow = clock()
        guard lookupNow.timeIntervalSince1970.isFinite,
              lookupNow.timeIntervalSince1970 >= 0 else {
            return .miss
        }
        let cutoff = lookupNow.addingTimeInterval(-config.entryMaxAge)
        try await storage.purgeStale(olderThan: cutoff)
        guard evidenceGeneration == expectedEvidenceGeneration else {
            return .miss
        }

        let revokedFingerprints = try await storage.fetchRevokedFingerprints(
            showId: normalizedShowId
        )
        guard evidenceGeneration == expectedEvidenceGeneration else {
            return .miss
        }
        if revokedFingerprints.contains(where: {
            $0.hammingDistance(to: fingerprint)
                <= config.hammingDistanceThreshold
        }) {
            return .miss
        }

        let candidates = try await storage.fetchAll(showId: normalizedShowId)
#if DEBUG
        if let barrier = lookupCandidateSnapshotBarrierForTesting {
            await barrier()
        }
#endif
        guard evidenceGeneration == expectedEvidenceGeneration else {
            return .miss
        }

        // Select by fingerprint accuracy first. Recency is an eviction/LRU
        // concern, not a similarity score: a newer threshold-edge creative
        // must never outrank an older exact match.
        let rankedCandidates = candidates.compactMap {
            candidate -> (entry: RepeatedAdCacheEntry, distance: Int)? in
            let sourceAssetId =
                RecurrenceMaterialIdentity.canonicalIdentifier(
                    candidate.sourceAssetId
                )
            let sourceWindowId =
                RecurrenceMaterialIdentity.canonicalIdentifier(
                    candidate.sourceWindowId
                )
            guard candidate.showId == normalizedShowId,
                  !candidate.fingerprint.isZero,
                  candidate.boundaryStart.isFinite,
                  candidate.boundaryEnd.isFinite,
                  candidate.boundaryStart >= 0,
                  candidate.boundaryEnd > candidate.boundaryStart,
                  candidate.confidence.isFinite,
                  (0...1).contains(candidate.confidence),
                  candidate.lastSeenAt.timeIntervalSince1970.isFinite,
                  candidate.lastSeenAt.timeIntervalSince1970 >= 0,
                  candidate.learningSource.authoritativeLifecycle
                    == candidate.learningLifecycle,
                  sourceAssetId != nil,
                  sourceWindowId != nil,
                  RecurrenceMaterialIdentity.canonicalIdentifier(
                      candidate.producerRevision
                  ) != nil
            else {
                return nil
            }
            let distance = candidate.fingerprint.hammingDistance(
                to: fingerprint
            )
            guard !revokedFingerprints.contains(where: {
                      $0.hammingDistance(to: candidate.fingerprint)
                          <= config.hammingDistanceThreshold
                  }),
                  distance <= config.hammingDistanceThreshold,
                  candidate.confidence >= config.storeConfidenceThreshold
            else {
                return nil
            }
            return (candidate, distance)
        }.sorted {
            if $0.distance != $1.distance {
                return $0.distance < $1.distance
            }
            if $0.entry.lastSeenAt != $1.entry.lastSeenAt {
                return $0.entry.lastSeenAt > $1.entry.lastSeenAt
            }
            return $0.entry.fingerprint.bits < $1.entry.fingerprint.bits
        }
        guard let candidate = rankedCandidates.first?.entry else {
            return .miss
        }

        // Bump LRU clock and return. `recordOutcome` remains the caller's
        // responsibility because lookup may be exploratory.
        let observedNow = clock()
        guard observedNow.timeIntervalSince1970.isFinite,
              observedNow.timeIntervalSince1970 >= 0 else {
            return .miss
        }
        let touchedNow = max(candidate.lastSeenAt, observedNow)
        try await storage.touch(
            showId: normalizedShowId,
            fingerprint: candidate.fingerprint,
            at: touchedNow
        )
        guard evidenceGeneration == expectedEvidenceGeneration else {
            return .miss
        }
        let touched = RepeatedAdCacheEntry(
            showId: candidate.showId,
            fingerprint: candidate.fingerprint,
            boundaryStart: candidate.boundaryStart,
            boundaryEnd: candidate.boundaryEnd,
            confidence: candidate.confidence,
            lastSeenAt: touchedNow,
            learningSource: candidate.learningSource,
            learningLifecycle: candidate.learningLifecycle,
            sourceAssetId: candidate.sourceAssetId,
            sourceWindowId: candidate.sourceWindowId,
            producerRevision: candidate.producerRevision
        )
        return .hit(touched)
    }

    /// Tombstone the corrected source and, when available, its same-show
    /// creative fingerprint. Corrections call this even while the cache is
    /// disabled: a disabled service may still own rows from a prior launch,
    /// and re-enabling must not resurrect a vetoed span.
    @discardableResult
    func revokeMatches(
        showId: String?,
        fingerprint: RepeatedAdFingerprint?,
        sourceAssetId: String,
        sourceWindowId: String,
        sourceStartTime: Double? = nil,
        sourceEndTime: Double? = nil,
        source: CatalogRevocationSource,
        at: Date? = nil
    ) async throws -> Int {
        evidenceGeneration &+= 1
        let normalizedSourceAssetId =
            RecurrenceMaterialIdentity.canonicalIdentifier(sourceAssetId)
        let normalizedSourceWindowId =
            RecurrenceMaterialIdentity.canonicalIdentifier(sourceWindowId)
        let revokedAt = at ?? clock()
        guard let normalizedSourceAssetId,
              let normalizedSourceWindowId,
              revokedAt.timeIntervalSince1970.isFinite,
              revokedAt.timeIntervalSince1970 >= 0 else {
            return 0
        }
        guard RecurrenceMaterialIdentity.tombstoneWindowKey(
            sourceWindowId: normalizedSourceWindowId,
            sourceStartTime: sourceStartTime,
            sourceEndTime: sourceEndTime
        ) != nil else {
            throw RepeatedAdCacheError.storageFailure(
                "invalid exact revocation material"
            )
        }

        let validShowId =
            RecurrenceMaterialIdentity.canonicalIdentifier(showId)
        let validFingerprint = fingerprint.flatMap {
            $0.isZero ? nil : $0
        }

        // Exact provenance remains the fallback when feature extraction or
        // show identity is unavailable. Storage owns the transaction so an
        // SQLite failure cannot commit only the tombstone or only a delete.
        return try await storage.revokeMatchesAtomically(
            showId: validShowId != nil && validFingerprint != nil
                ? validShowId
                : nil,
            fingerprint: validShowId != nil && validFingerprint != nil
                ? validFingerprint
                : nil,
            sourceAssetId: normalizedSourceAssetId,
            sourceWindowId: normalizedSourceWindowId,
            sourceStartTime: sourceStartTime,
            sourceEndTime: sourceEndTime,
            source: source,
            at: revokedAt
        )
    }

    // MARK: Hit-rate window + auto-disable

    /// Record a single user-facing outcome (hit or miss). Drives the
    /// 14-day rolling window and triggers auto-disable when the
    /// configured floor is breached.
    ///
    /// Write-rate envelope (playhead-43ed M2): each call is one
    /// SQLite INSERT into `repeated_ad_cache_outcomes` plus a bounded
    /// `purgeOutcomes` DELETE. After the C2 fix, the
    /// `AdDetectionService` hot path only records a miss when the
    /// classifier verdict clears `storeConfidenceThreshold` (i.e. an
    /// actual ad the cache failed to recognise). On a typical episode
    /// that's O(adCount) writes — single-digit per episode — well
    /// inside the unbatched-INSERT envelope (<1ms each on iPhone 17
    /// Pro). No transaction/batching wrapper is needed; the bottleneck
    /// would be the classifier itself, which already runs at >100ms
    /// per candidate. If a future change removes the C2 verdict gate
    /// — which would resurrect the original "every miss writes a
    /// row" behaviour — revisit batching here.
    func recordOutcome(hit: Bool) async throws {
        guard enabled, !clearInProgress else { return }
        outcomeOperationsInFlight += 1
        let autoDisableRequest:
            (rate: Double, samples: Int, evidenceGeneration: UInt64)?
        do {
            autoDisableRequest = try await recordTrackedOutcome(hit: hit)
        } catch {
            finishOutcomeOperation()
            throw error
        }
        finishOutcomeOperation()
        if let autoDisableRequest {
#if DEBUG
            if let barrier = autoDisableRequestBarrierForTesting {
                await barrier()
            }
#endif
            await autoDisable(
                observedHitRate: autoDisableRequest.rate,
                samples: autoDisableRequest.samples,
                expectedEvidenceGeneration:
                    autoDisableRequest.evidenceGeneration
            )
        }
    }

    private func recordTrackedOutcome(
        hit: Bool
    ) async throws -> (
        rate: Double,
        samples: Int,
        evidenceGeneration: UInt64
    )? {
        let expectedEvidenceGeneration = evidenceGeneration
        let now = clock()
        guard now.timeIntervalSince1970.isFinite,
              now.timeIntervalSince1970 >= 0 else {
            return nil
        }
        try await storage.appendOutcome(.init(timestamp: now, isHit: hit))
        guard evidenceGeneration == expectedEvidenceGeneration,
              enabled,
              !clearInProgress else {
            return nil
        }

        // Trim outside the active window so the table size is bounded.
        let cutoff = now.addingTimeInterval(-config.autoDisableWindow)
        try await storage.purgeOutcomes(olderThan: cutoff)
        guard evidenceGeneration == expectedEvidenceGeneration,
              enabled,
              !clearInProgress else {
            return nil
        }

        let snapshot = try await computeHitRateSnapshot(now: now)
        guard evidenceGeneration == expectedEvidenceGeneration,
              enabled,
              !clearInProgress else {
            return nil
        }
        // Auto-disable: only after enough samples have accumulated.
        if snapshot.totalSamples >= config.autoDisableMinSamples,
           let rate = snapshot.hitRate,
           rate < config.autoDisableHitRateFloor {
            return (rate, snapshot.totalSamples, expectedEvidenceGeneration)
        }
        return nil
    }

    /// Hit-rate snapshot for the current 14-day window. Re-computed
    /// from persisted outcomes so a launch sees the correct value
    /// even if `recordOutcome` was never called this session.
    func currentHitRateSnapshot() async throws -> RepeatedAdCacheHitRateSnapshot {
        let now = clock()
        guard now.timeIntervalSince1970.isFinite,
              now.timeIntervalSince1970 >= 0 else {
            throw RepeatedAdCacheError.storageFailure("invalid clock")
        }
        let snapshot = try await computeHitRateSnapshot(now: now)
        return snapshot
    }

    private func computeHitRateSnapshot(now: Date) async throws -> RepeatedAdCacheHitRateSnapshot {
        let cutoff = now.addingTimeInterval(-config.autoDisableWindow)
        let samples = try await storage.fetchOutcomes(newerThan: cutoff)
        let hitCount = samples.filter { $0.isHit }.count
        return RepeatedAdCacheHitRateSnapshot(
            windowSeconds: config.autoDisableWindow,
            totalSamples: samples.count,
            hitCount: hitCount
        )
    }

    private func autoDisable(
        observedHitRate: Double,
        samples: Int,
        expectedEvidenceGeneration: UInt64
    ) async {
        guard enabled,
              !clearInProgress,
              evidenceGeneration == expectedEvidenceGeneration else {
            return
        }
        evidenceGeneration &+= 1
        enablementGeneration &+= 1
        requestedEnabled = false
        enabled = false
        let transitionGeneration = enablementGeneration
        disableReason = .autoDisabledLowHitRate(
            observedHitRate: observedHitRate,
            samples: samples
        )
        await acquireLifecycleCleanup()
        defer { releaseLifecycleCleanup() }
        await waitForOutcomeOperationsToDrain()
        guard enablementGeneration == transitionGeneration,
              !requestedEnabled else {
            return
        }
        do {
            try await storage.clearEntries()
#if DEBUG
            if let barrier = lifecycleCleanupBarrierForTesting {
                await barrier()
            }
#endif
            guard enablementGeneration == transitionGeneration,
                  !requestedEnabled else {
                return
            }
            try await storage.clearOutcomes()
        } catch {
            guard enablementGeneration == transitionGeneration,
                  !requestedEnabled else {
                return
            }
            logger.error(
                "RepeatedAdCache: auto-disable cleanup failed; cache remains disabled: \(String(describing: error), privacy: .public)"
            )
        }
        guard enablementGeneration == transitionGeneration,
              !requestedEnabled else {
            return
        }
        logger.info("RepeatedAdCache: auto-disabled (rate=\(observedHitRate, privacy: .public), samples=\(samples, privacy: .public))")
        // Notify the embedder so it can persist the disabled state
        // across launches (bead §4). Defaulted to a no-op so unit
        // tests that don't care about persistence don't have to wire
        // it up.
        onAutoDisable(observedHitRate, samples)
    }

    // MARK: Maintenance

    /// Purge entries older than `entryMaxAge`. Idempotent.
    @discardableResult
    func purgeStaleEntries() async throws -> Int {
        let now = clock()
        guard now.timeIntervalSince1970.isFinite,
              now.timeIntervalSince1970 >= 0 else {
            return 0
        }
        let cutoff = now.addingTimeInterval(-config.entryMaxAge)
        return try await storage.purgeStale(olderThan: cutoff)
    }

    /// Force-clear everything (entries + outcomes). Used by Diagnostics
    /// "Clear cache" affordance.
    func clear() async throws {
        evidenceGeneration &+= 1
        clearOperationsInFlight += 1
        defer {
            precondition(clearOperationsInFlight > 0)
            clearOperationsInFlight -= 1
        }
        await waitForOutcomeOperationsToDrain()
        try await storage.clearEntries()
#if DEBUG
        if let barrier = maintenanceClearBarrierForTesting {
            await barrier()
        }
#endif
        try await storage.clearOutcomes()
        logger.info("RepeatedAdCache: cleared by explicit request")
    }

    private func finishOutcomeOperation() {
        precondition(outcomeOperationsInFlight > 0)
        outcomeOperationsInFlight -= 1
        guard outcomeOperationsInFlight == 0 else { return }
        let waiters = outcomeDrainWaiters
        outcomeDrainWaiters.removeAll()
        for waiter in waiters {
            waiter.resume()
        }
    }

    private func waitForOutcomeOperationsToDrain() async {
        guard outcomeOperationsInFlight > 0 else { return }
        await withCheckedContinuation { continuation in
            outcomeDrainWaiters.append(continuation)
#if DEBUG
            let observers = outcomeDrainObserversForTesting
            outcomeDrainObserversForTesting.removeAll()
            for observer in observers {
                observer.resume()
            }
#endif
        }
    }

    private func acquireLifecycleCleanup() async {
        guard lifecycleCleanupOwned else {
            lifecycleCleanupOwned = true
            return
        }
        await withCheckedContinuation { continuation in
            lifecycleCleanupWaiters.append(continuation)
#if DEBUG
            let observers = lifecycleCleanupWaitObserversForTesting
            lifecycleCleanupWaitObserversForTesting.removeAll()
            for observer in observers {
                observer.resume()
            }
#endif
        }
    }

    private func releaseLifecycleCleanup() {
        precondition(lifecycleCleanupOwned)
        guard !lifecycleCleanupWaiters.isEmpty else {
            lifecycleCleanupOwned = false
            return
        }
        let next = lifecycleCleanupWaiters.removeFirst()
        // Ownership transfers directly to the resumed waiter.
        next.resume()
    }
}
