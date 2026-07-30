// DiagnosticsBundleBuilder.swift
// Pure transform from raw inputs (work-journal entries, eligibility,
// per-episode opt-in inputs) into the support-safe diagnostics bundle.
//
// Scope: playhead-ghon (Phase 1.5 — support-safe diagnostics bundle classes).
//        playhead-au2v.1.3 (chapter signal diagnostics events — additive
//        `chapter_phase_events` parameter on `buildDefault`).
//
// Why a free `enum` of pure statics: the builder has no instance state
// and the legal checklist demands deterministic, audit-able transforms.
// Keeping the surface as static functions makes every code path
// reachable from a unit test without spinning up an actor.
//
// Input ordering contract: the builder does NOT assume any particular
// ordering of the `workJournalEntries` input. Both the `scheduler_events`
// and `work_journal_tail` projections sort by `timestamp` ascending
// internally before taking the most-recent tail. This keeps the builder
// correct whether the caller supplies rows oldest-first (insertion
// order, as the spec's `work_journal_tail[]` language implies) or
// newest-first (which is what the production `AnalysisStore`
// `ORDER BY timestamp DESC, rowid DESC` fetch returns). Without the
// internal sort a `.suffix(N)` on a DESC-ordered input would silently
// return the OLDEST N rows of the fetched window.
//
// Legal checklist enforcement points (per spec):
//   (a) Default bundle never carries a raw episodeId — all references
//       go through `EpisodeIdHasher.hash(installID:episodeId:)`.
//   (b) Transcript excerpts use the ±30 s window and 1000-char
//       truncation defined as `Self.transcriptExcerptHalfWindow` and
//       `Self.transcriptExcerptCharCap`.
//   (c) Hashing scheme is delegated to `EpisodeIdHasher`.
//   (d) Feature summaries are restricted to `OptInBundle.FeatureSummary`
//       (mean / max only); the input shape physically cannot carry a
//       raw feature vector.
//   (g) Banner tallies (playhead-bfq7) reach the bundle only through
//       `DefaultBundle.BannerTallySummary`, whose episode reference is
//       produced here by `EpisodeIdHasher`. The raw episode id stops
//       at this function.

import Foundation

// MARK: - Input shapes (test-friendly intermediates)

/// One transcript chunk, narrowed to the only fields the diagnostics
/// builder cares about. Keeping this distinct from `TranscriptChunk`
/// (which lives in `AnalysisStore.swift` and carries far more analyzer
/// state) decouples the builder from the SQLite store and makes the
/// pure-function tests cheap.
struct DiagnosticsTranscriptChunk: Sendable, Equatable {
    let startTime: Double
    let endTime: Double
    let text: String
}

/// playhead-p70f: the raw, un-projected rediff telemetry the builder turns into
/// `DefaultBundle.RediffDiagnostics`. Kept as store-native types so the fetch
/// closure is a straight read and the ONE place raw asset ids are dropped is
/// `buildDefault` (legal checklist item a — same discipline as banner tallies).
struct DiagnosticsRediffSnapshot: Sendable {
    var bandwidth: RediffBandwidthTotals = RediffBandwidthTotals()
    var refetchStates: [RediffRefetchStateRow] = []
    var dayZeroAttempts: [RediffDayZeroAttemptRecord] = []
    var backgroundRuns: [BackgroundTaskRunRecord] = []
    /// Names (from `RediffDiagnosticsFetchAdapter.Read`) of the reads that
    /// THREW. Empty is the healthy case. Without this an unreadable table and
    /// an empty table are the same bundle — the "zero is not evidence" mistake
    /// this bead exists to correct, reintroduced at the export layer.
    var readFailures: [String] = []

    static let empty = DiagnosticsRediffSnapshot()
}

/// Per-episode input for `DiagnosticsBundleBuilder.buildOptIn(...)`.
struct DiagnosticsEpisodeInput: Sendable, Equatable {
    let episodeId: String
    let episodeTitle: String
    let diagnosticsOptIn: Bool
    /// Detected ad boundary times in seconds. The builder emits one
    /// transcript excerpt per boundary.
    let adBoundaryTimes: [Double]
    /// Persisted transcript chunks for this episode, in increasing time
    /// order. The builder does not re-sort.
    let transcriptChunks: [DiagnosticsTranscriptChunk]
    /// Pre-aggregated coarse feature summary for this episode. `nil`
    /// when the episode has no completed feature pass.
    let featureSummary: OptInBundle.FeatureSummary?
}

// MARK: - Builder

enum DiagnosticsBundleBuilder {

    /// Number of `WorkJournalEntry` rows projected into
    /// `scheduler_events`. Per bead spec.
    static let schedulerEventsCap = 200

    /// Number of `WorkJournalEntry` rows preserved in
    /// `work_journal_tail`. Per bead spec.
    static let workJournalTailCap = 50

    // MARK: - Failure recovery (playhead-8ysk)

    /// Recover the closed-vocabulary transcription-failure fields from a
    /// journal row's free-form `metadata` blob.
    ///
    /// This is the ONLY thing the builder ever reads out of `metadata`, and
    /// it is a whitelist in the strongest sense available: `failure_class`
    /// has to round-trip through `TranscriptFailureClass(rawValue:)` — a set
    /// of compile-time string literals — or it is dropped, and
    /// `failure_code` has to parse as an `Int` or it is dropped. A future
    /// emitter that writes an error message under either key exports
    /// nothing, which is the property that lets these two fields cross the
    /// projection boundary that `metadata` as a whole correctly may not.
    static func extractFailure(
        fromMetadata metadata: String
    ) -> (
        failureClass: String?,
        failureCode: Int?,
        failureObservation: String?,
        failureTermination: String?
    ) {
        guard let data = metadata.data(using: .utf8),
              let object = try? JSONSerialization.jsonObject(with: data),
              let dict = object as? [String: Any]
        else {
            return (nil, nil, nil, nil)
        }
        let failureClass = (dict[DiagnosticsFailureKeys.failureClass] as? String)
            .flatMap(TranscriptFailureClass.init(rawValue:))?
            .rawValue
        // The writer encodes every extra as a string; accept a JSON number
        // too so a future typed emitter is not silently dropped.
        let rawCode = dict[DiagnosticsFailureKeys.failureCode]
        let failureCode = (rawCode as? String).flatMap(Int.init)
            ?? (rawCode as? NSNumber)?.intValue
        // playhead-ngev: identical construction to `failure_class` — the value
        // is admitted ONLY if it round-trips through a closed enum of
        // compile-time literals, so an emitter that writes free text under
        // either key exports nothing at all.
        let failureObservation = (dict[DiagnosticsFailureKeys.failureObservation] as? String)
            .flatMap(AnalysisJobRunner.TranscriptRunObservation.init(rawValue:))?
            .rawValue
        let failureTermination = (dict[DiagnosticsFailureKeys.failureTermination] as? String)
            .flatMap(TranscriptRunTermination.init(rawValue:))?
            .rawValue
        return (failureClass, failureCode, failureObservation, failureTermination)
    }

    /// Half of the transcript excerpt window around an ad boundary;
    /// total window is `2 * halfWindow` seconds. Locked in at 30 s by
    /// legal checklist item (b).
    static let transcriptExcerptHalfWindow: Double = 30

    /// Hard cap on the character length of a single transcript excerpt.
    /// Excerpts longer than this are truncated to the cap. Legal
    /// checklist item (b).
    ///
    /// Units: grapheme-cluster count via `String.prefix` (i.e.
    /// `String.count`). Byte-length of the resulting UTF-8 may exceed
    /// this for multi-byte characters (emoji, CJK text, combining
    /// marks). This is intentional — the cap is about limiting the
    /// amount of *text* shipped for legal review, not bytes on disk.
    static let transcriptExcerptCharCap = 1_000

    // MARK: - Default bundle

    /// Pure transform from raw inputs into the always-safe
    /// `DefaultBundle`.
    ///
    /// Input order is not significant: the builder sorts
    /// `workJournalEntries` by `timestamp` ascending before taking
    /// the tail for both `scheduler_events` and `work_journal_tail`,
    /// so it produces the same spec-compliant output regardless of
    /// whether the caller supplies oldest-first (insertion order) or
    /// newest-first (`ORDER BY timestamp DESC`) rows. This guards
    /// against the `.suffix(N)` inversion bug where a DESC-ordered
    /// caller would otherwise leak the OLDEST N rows into the tail.
    ///
    /// `chapterPhaseEvents` is passed through unchanged (no sort, no
    /// cap). Capping is the caller's responsibility — it lives in the
    /// persistence-backed fetch closure that the consumer beads will
    /// land (mirrors how `WorkJournalEntry` capping is delegated to
    /// the `journalFetch` / `analysisStore` query, NOT this builder).
    /// Adding a cap here without a use case would force a sort key
    /// (timestamp? generation_id?) decision before the consumer beads
    /// have shipped.
    static func buildDefault(
        appVersion: String,
        osVersion: String,
        deviceClass: DeviceClass,
        buildType: BuildType,
        eligibility: AnalysisEligibility,
        workJournalEntries: [WorkJournalEntry],
        installID: UUID,
        chapterPhaseEvents: [ChapterPhaseEvent] = [],
        musicBedProfileSnapshots: [ShowMusicBedProfileSnapshot] = [],
        learnedDeviceProfiles: [LearnedDeviceProfileDiagnosticRecord] = [],
        stabilityDiagnostics: [StabilityDiagnosticRecord] = [],
        bannerTallies: [BannerTallySession] = [],
        rediff: DiagnosticsRediffSnapshot = .empty,
        analysisStoreHealth: AnalysisStoreHealthState = .healthy,
        speechModelLoad: SpeechModelLoadState = .unknown
    ) -> DefaultBundle {

        // Canonicalise: timestamp ASCENDING (oldest first). Taking the
        // suffix of this ordering is equivalent to "most recent N",
        // independent of how the caller sorted the input. Reversing
        // gives us newest-first for the `scheduler_events` projection.
        let sortedAsc = workJournalEntries.sorted { $0.timestamp < $1.timestamp }

        // scheduler_events: most-recent N by timestamp, emitted newest
        // first. Take the trailing N of the ascending list and reverse.
        let schedulerTailAsc = sortedAsc.suffix(schedulerEventsCap)
        let schedulerEvents = schedulerTailAsc.reversed().map { entry -> DefaultBundle.SchedulerEvent in
            DefaultBundle.SchedulerEvent(
                timestamp: entry.timestamp,
                eventType: entry.eventType.rawValue,
                episodeIdHash: EpisodeIdHasher.hash(
                    installID: installID, episodeId: entry.episodeId
                ),
                internalMissCause: entry.cause?.rawValue
            )
        }

        // work_journal_tail: most-recent N by timestamp, emitted in
        // ascending (insertion-equivalent) order — the spec phrases
        // this as "last 50 … by insertion order".
        let tailSlice = sortedAsc.suffix(workJournalTailCap)
        let workJournalTail = tailSlice.map { entry -> DefaultBundle.WorkJournalRecord in
            let failure = extractFailure(fromMetadata: entry.metadata)
            return DefaultBundle.WorkJournalRecord(
                id: entry.id,
                episodeIdHash: EpisodeIdHasher.hash(
                    installID: installID, episodeId: entry.episodeId
                ),
                generationID: entry.generationID.uuidString,
                schedulerEpoch: entry.schedulerEpoch,
                timestamp: entry.timestamp,
                eventType: entry.eventType.rawValue,
                cause: entry.cause?.rawValue,
                failureClass: failure.failureClass,
                failureCode: failure.failureCode,
                failureObservation: failure.failureObservation,
                failureTermination: failure.failureTermination
            )
        }

        let reason = AnalysisUnavailableReason.derive(from: eligibility)

        // playhead-2hpn: project each show-music-bed snapshot into a
        // support-safe summary. The show identifier is the same opaque
        // per-install hash used for episode IDs (legal checklist item a
        // — never the raw catalogue identifier). The audio-derived
        // hash bits themselves are omitted; only the count is exposed.
        let musicBedProfiles = musicBedProfileSnapshots.map { snapshot in
            DefaultBundle.MusicBedProfileSummary(
                showIdentifierHash: EpisodeIdHasher.hash(
                    installID: installID, episodeId: snapshot.showIdentifier
                ),
                confirmationCount: snapshot.confirmationCount,
                consecutiveMissCount: snapshot.consecutiveMissCount,
                storedHashCount: snapshot.confirmedJingleHashes.count,
                isConfirmed: snapshot.isConfirmed,
                versionStamp: snapshot.versionStamp
            )
        }

        // playhead-bfq7: this is the ONLY projection of a
        // `BannerTallySession`, and it is where the raw episode id is
        // dropped. The store keeps the raw id (it is local-only); the
        // bundle gets the same install-scoped hash the scheduler-event
        // and work-journal tails use (legal checklist item a). Nothing
        // else about the card — title, feed, advertiser, window id,
        // session key — is carried across.
        let bannerTallySummaries = bannerTallies.map { session in
            DefaultBundle.BannerTallySummary(
                episodeIdHash: EpisodeIdHasher.hash(
                    installID: installID, episodeId: session.episodeId
                ),
                bannerCount: session.bannerCount,
                autoSkippedCount: session.autoSkippedCount,
                suggestCount: session.suggestCount,
                firstShownAt: session.firstShownAt.timeIntervalSince1970,
                lastShownAt: session.lastShownAt.timeIntervalSince1970
            )
        }

        let rediffDiagnostics = projectRediff(rediff, installID: installID)

        return DefaultBundle(
            appVersion: appVersion,
            osVersion: osVersion,
            deviceClass: deviceClass,
            buildType: buildType,
            eligibilitySnapshot: eligibility,
            analysisUnavailableReason: reason,
            schedulerEvents: schedulerEvents,
            workJournalTail: Array(workJournalTail),
            chapterPhaseEvents: chapterPhaseEvents,
            musicBedProfiles: musicBedProfiles,
            learnedDeviceProfiles: learnedDeviceProfiles,
            // playhead-jw63.4: passed through unchanged. The store has
            // already applied the ring-buffer cap and the projector has
            // already applied the allowlist, so re-deriving either here
            // would only create a second place for them to disagree.
            stabilityDiagnostics: stabilityDiagnostics,
            bannerTallies: bannerTallySummaries,
            rediffDiagnostics: rediffDiagnostics,
            // playhead-wvdz: passed through unchanged. The journal has
            // already applied its record caps and its detail allowlist,
            // so re-deriving either here would only create a second
            // place for them to disagree — the same reasoning as
            // `stabilityDiagnostics` above. There is nothing to hash:
            // the shape carries no episode, asset, or show reference.
            analysisStoreHealth: analysisStoreHealth,
            speechModelLoad: speechModelLoad
        )
    }

    // MARK: - Rediff lane projection (playhead-p70f)

    /// Cap on how many per-asset rediff rows ship. A large library could carry
    /// hundreds; the newest are the ones a support engineer reads.
    static let rediffRowCap = 50

    /// Cap on `background_task_runs` rows for the rediff entry point.
    static let rediffBackgroundRunCap = 25

    /// Project the raw rediff telemetry into the support-safe bundle shape.
    ///
    /// This is the ONLY place a raw `analysisAssetId` is dropped: every row's
    /// id goes through `EpisodeIdHasher` (legal checklist item a), exactly as
    /// the banner-tally and music-bed projections do.
    ///
    /// The day-0 rows' `lastDetail` — the only free text in the snapshot — is
    /// NOT projected at all rather than sanitized: it originates from
    /// `String(describing: error)`, which on a `URLError` carries the enclosure
    /// URL, and no allowlist is a safe bound on an arbitrary error dump. The
    /// closed `last_exit` enum is what a support engineer needs; the free text
    /// stays on device. `DiagnosticsBundleRediffTests.detailIsNotExported` is
    /// the proof, and every other projected field is an integer, a timestamp,
    /// or a closed enum `rawValue`.
    private static func projectRediff(
        _ snapshot: DiagnosticsRediffSnapshot,
        installID: UUID
    ) -> DefaultBundle.RediffDiagnostics {
        func hash(_ assetId: String) -> String {
            EpisodeIdHasher.hash(installID: installID, episodeId: assetId)
        }

        let bandwidth = DefaultBundle.RediffBandwidthSummary(
            precheckBytesTotal: snapshot.bandwidth.precheckBytesTotal,
            fullFetchBytesTotal: snapshot.bandwidth.fullFetchBytesTotal,
            unchangedCount: snapshot.bandwidth.unchangedCount,
            rotatedCount: snapshot.bandwidth.rotatedCount,
            failedCount: snapshot.bandwidth.failedCount,
            parkedCount: snapshot.bandwidth.parkedCount,
            dayZeroUnmarkedCount: snapshot.bandwidth.dayZeroUnmarkedCount,
            lastUpdatedAt: snapshot.bandwidth.lastUpdatedAt
        )

        let refetchStates = snapshot.refetchStates
            .sorted { $0.updatedAt > $1.updatedAt }
            .prefix(rediffRowCap)
            .map { row in
                DefaultBundle.RediffRefetchStateSummary(
                    assetIdHash: hash(row.analysisAssetId),
                    unchangedAttempts: row.attemptState.unchangedAttempts,
                    lastAttemptAt: row.attemptState.lastAttemptAt,
                    resolved: row.attemptState.resolved,
                    lastFailureClass: row.attemptState.lastFailureClass?.rawValue,
                    sameClassFailureStreak: row.attemptState.sameClassFailureStreak,
                    updatedAt: row.updatedAt
                )
            }

        let dayZeroAttempts = snapshot.dayZeroAttempts
            .sorted { $0.lastAttemptAt > $1.lastAttemptAt }
            .prefix(rediffRowCap)
            .map { row in
                DefaultBundle.RediffDayZeroAttemptSummary(
                    assetIdHash: hash(row.analysisAssetId),
                    attemptCount: row.attemptCount,
                    lastAttemptAt: row.lastAttemptAt,
                    lastExit: row.lastExit.rawValue,
                    lastMarkCount: row.lastMarkCount,
                    lastBSideCount: row.lastBSideCount,
                    lastBSidesAccepted: row.lastBSidesAccepted,
                    lastBSidesGateRejected: row.lastBSidesGateRejected,
                    lastBSidesUnreadable: row.lastBSidesUnreadable,
                    lastDivergentSlotCount: row.lastDivergentSlotCount,
                    lastFullFetchBytes: row.lastFullFetchBytes,
                    totalFullFetchBytes: row.totalFullFetchBytes,
                    suppressedCount: row.suppressedCount,
                    lastSuppressedAt: row.lastSuppressedAt,
                    policyGeneration: row.policyGeneration
                )
            }

        let backgroundRuns = snapshot.backgroundRuns
            .filter { $0.entryPoint == .rediffRefetch }
            .sorted { $0.startedAt > $1.startedAt }
            .prefix(rediffBackgroundRunCap)
            .map { run in
                DefaultBundle.RediffBackgroundRunSummary(
                    startedAt: run.startedAt,
                    finishedAt: run.finishedAt,
                    outcome: run.outcome.rawValue,
                    precheckBytes: rediffAnnotationValue(run.deferReason, key: "precheckBytes"),
                    fullFetchBytes: rediffAnnotationValue(run.deferReason, key: "fullFetchBytes"),
                    jobsSeen: run.jobsSeen,
                    jobsAdmitted: run.jobsAdmitted,
                    jobsCompleted: run.jobsCompleted,
                    expiration: run.expiration
                )
            }

        // Closed vocabulary only — anything the adapter did not name is
        // dropped rather than forwarded, so this can never become a text
        // channel out of the device.
        let readFailures = snapshot.readFailures
            .compactMap { RediffDiagnosticsFetchAdapter.Read(rawValue: $0)?.rawValue }

        return DefaultBundle.RediffDiagnostics(
            bandwidth: bandwidth,
            refetchStates: Array(refetchStates),
            dayZeroAttempts: Array(dayZeroAttempts),
            backgroundRuns: Array(backgroundRuns),
            readFailures: readFailures
        )
    }

    /// Longest `deferReason` this parser will scan. The rediff annotation is
    /// two `key=integer` pairs; anything longer is not it.
    static let rediffAnnotationCharCap = 120

    /// Pull ONE `key=<integer>` value out of the rediff sweep's run-ledger
    /// annotation (`"precheckBytes=N fullFetchBytes=M"`), or `nil`.
    ///
    /// NO free text crosses this boundary — the return type is `Int?`. Two
    /// reasons the annotation is parsed rather than forwarded:
    ///   * `DiagnosticTextSanitizer`'s allowlist has no `=`, so the raw string
    ///     would be rejected wholesale and the bandwidth signal lost;
    ///   * `deferReason` is a free-form column shared with other entry points,
    ///     so "forward whatever is in it" is not a bound anyone maintains.
    ///
    /// The day-0 records' `lastDetail` is likewise NOT exported: it is
    /// `String(describing: error)`, and a `URLError` carries the enclosure URL
    /// — a content-identifying string that would be a stronger disclosure than
    /// the raw `episodeId` legal checklist item (a) already forbids. The closed
    /// `last_exit` enum is what a support engineer needs; the free text stays
    /// on device in `rediff_day_zero_attempts`.
    private static func rediffAnnotationValue(_ raw: String?, key: String) -> Int? {
        guard let raw, raw.count <= rediffAnnotationCharCap else { return nil }
        for token in raw.split(separator: " ") {
            let parts = token.split(separator: "=", maxSplits: 1, omittingEmptySubsequences: false)
            guard parts.count == 2, parts[0] == key else { continue }
            return Int(parts[1])
        }
        return nil
    }

    // MARK: - OptIn bundle

    /// Returns an `OptInBundle` containing only opted-in episodes.
    /// Returns `nil` when no input has `diagnosticsOptIn == true` so the
    /// surrounding `DiagnosticsBundleFile` can omit the field entirely
    /// (clearer than emitting an empty `episodes: []` array).
    static func buildOptIn(episodes: [DiagnosticsEpisodeInput]) -> OptInBundle? {
        let optedIn = episodes.filter(\.diagnosticsOptIn)
        guard !optedIn.isEmpty else { return nil }

        let mapped = optedIn.map { input -> OptInBundle.Episode in
            let excerpts = input.adBoundaryTimes.map { boundary in
                makeExcerpt(boundary: boundary, chunks: input.transcriptChunks)
            }
            return OptInBundle.Episode(
                episodeId: input.episodeId,
                episodeTitle: input.episodeTitle,
                transcriptExcerpts: excerpts,
                featureSummaries: input.featureSummary.map { [$0] } ?? []
            )
        }
        return OptInBundle(episodes: mapped)
    }

    // MARK: - Transcript excerpt window + truncation

    /// Build a single excerpt for one boundary: take chunks whose
    /// `[startTime, endTime]` overlaps `[boundary - 30s, boundary + 30s]`,
    /// concatenate their text with a space separator, and truncate to
    /// the 1000-char cap.
    private static func makeExcerpt(
        boundary: Double,
        chunks: [DiagnosticsTranscriptChunk]
    ) -> OptInBundle.TranscriptExcerpt {
        let windowStart = boundary - transcriptExcerptHalfWindow
        let windowEnd = boundary + transcriptExcerptHalfWindow
        let included = chunks.filter { chunk in
            // Half-open interval semantics: the chunk overlaps the
            // window if its end is past the window start AND its start
            // is before the window end. This matches the FeatureWindow
            // overlap convention used elsewhere in the pipeline and
            // keeps boundary-aligned chunks IN the excerpt.
            chunk.endTime > windowStart && chunk.startTime < windowEnd
        }
        let raw = included.map(\.text).joined(separator: " ")
        let truncated = String(raw.prefix(transcriptExcerptCharCap))
        return OptInBundle.TranscriptExcerpt(
            boundaryTime: boundary,
            startTime: windowStart,
            endTime: windowEnd,
            text: truncated
        )
    }
}
