// TranscriptPeekViewModel.swift
// Drives the transcript peek sheet: pulls snapshots from a
// `TranscriptPeekDataSource`, polls for canonical transcript changes,
// resolves the active segment index from the current playback time,
// and identifies ad regions.
//
// playhead-fwvz: this file is on the UI-layer contract — it consumes
// the `TranscriptPeekSnapshot` boundary type only and does not
// reference `AnalysisStore` (or any other forbidden module-boundary
// token enforced by `SurfaceStatusUILintTests`). The fetch logic that
// previously lived here moved to `LiveTranscriptPeekDataSource`.

import Foundation
import OSLog

/// Immutable audio envelope captured when a visible transcript row is
/// selected. Polling may replace or reorder canonical rows before the user
/// confirms a mark, so submission state must not depend on a later array
/// offset.
struct TranscriptChunkSelection: Hashable, Sendable {
    let startTime: TimeInterval
    let endTime: TimeInterval

    init(chunk: TranscriptChunk) {
        startTime = chunk.startTime
        endTime = chunk.endTime
    }
}

@MainActor
@Observable
final class TranscriptPeekViewModel {

    // MARK: - State

    /// Canonical display chunks sorted by startTime.
    private(set) var chunks: [TranscriptChunk] = []

    /// Ad windows for visual muting of ad segments (legacy Phase 2 path).
    private(set) var adWindows: [AdWindow] = []

    /// Phase 5 decoded spans for the new overlay rendering.
    private(set) var decodedSpans: [DecodedSpan] = []

    /// Pre-computed mapping from chunk index to overlapping decoded spans.
    /// Rebuilt each refresh cycle so per-row lookups are O(1).
    private var spansByChunkIndex: [Int: [DecodedSpan]] = [:]

    /// Chunk indices that overlap a user-marked AdWindow (boundaryState "userMarked").
    /// These get visual ad highlighting even without a corresponding DecodedSpan.
    private var userMarkedChunkIndices: Set<Int> = []

    /// playhead-d666: the overlapping decoded spans that make an ad CLAIM about
    /// this row — every span in `spansByChunkIndex` except the ones anchored
    /// solely by a `.sustainedMusicOffset` hint. Absent (rather than empty) when
    /// nothing on the row claims.
    ///
    /// Kept SEPARATE from `spansByChunkIndex` (which still holds the music-only
    /// spans, so persistence and every non-display read are unchanged) because
    /// the only thing that must differ is what the row ASSERTS. Stored as the
    /// span list rather than a boolean because the AD badge needs the span
    /// IDENTITIES: it decides "is this the first row of this span" by comparing
    /// this row's ids against the previous row's, and a music-only id leaking
    /// into either set silently swallows the badge of the real span next to it
    /// (playhead-d666 R1).
    ///
    /// playhead-d666 R6: this is now the ONLY per-row span population any
    /// display surface reads — bar, tint, badge, spoken label and popover.
    private var adClaimingSpansByChunkIndex: [Int: [DecodedSpan]] = [:]

    /// Index of the chunk containing the current playback position, or nil.
    private(set) var activeChunkIndex: Int?

    /// Most recent playback position supplied by the view. A poll can change
    /// canonical row cardinality while playback is paused, so refresh uses
    /// this value to resolve the active row against the new projection.
    private var latestPlaybackPosition: TimeInterval?

    /// True while the initial load is in progress.
    private(set) var isLoading: Bool = true

    /// `AnalysisAsset.fastTranscriptCoverageEndTime` for the asset, or nil
    /// when not yet computed. This is the SCAN watermark: how far the fast
    /// pass has looked, which is not the same as how far it found speech
    /// (playhead-7tn8). It separates "not yet scanned" from "scanned, found
    /// nothing" for the untranscribed-tail mark affordance (playhead-m1l9).
    private(set) var fastTranscriptCoverageEndTime: TimeInterval?

    // MARK: - Configuration

    let analysisAssetId: String
    private let dataSource: TranscriptPeekDataSource
    private let logger = Logger(subsystem: "com.playhead", category: "TranscriptPeek")

    /// How often to poll for new chunks (seconds).
    /// Polling is intentional here: the data source does not emit
    /// granular notifications for individual chunk inserts. The
    /// 2-second interval balances responsiveness with efficiency.
    /// When the data source gains change notifications, this should
    /// be replaced with event-driven updates.
    private static let pollInterval: TimeInterval = 2.0

    private var pollTask: Task<Void, Never>?

    // MARK: - Init

    init(analysisAssetId: String, dataSource: TranscriptPeekDataSource) {
        self.analysisAssetId = analysisAssetId
        self.dataSource = dataSource
    }

    // MARK: - Lifecycle

    func startPolling() {
        pollTask?.cancel()
        pollTask = Task { [weak self] in
            guard let self else { return }
            // Initial load
            self.logger.info("Transcript peek: starting initial load for asset \(self.analysisAssetId)")
            let start = ContinuousClock.now
            await self.refresh()
            // `refresh()` rejects a cancelled generation's snapshot, but
            // this caller must also avoid publishing lifecycle state after
            // `startPolling()` has replaced it with a newer task.
            guard !Task.isCancelled else { return }
            self.isLoading = false
            self.logger.info("Transcript peek: initial load done in \(ContinuousClock.now - start), \(self.chunks.count) chunks")

            // Continuous polling for transcript projection changes.
            while !Task.isCancelled {
                try? await Task.sleep(for: .seconds(Self.pollInterval))
                guard !Task.isCancelled else { break }
                let before = self.chunks.count
                await self.refresh()
                let after = self.chunks.count
                if after != before {
                    self.logger.info("Transcript peek: \(before) → \(after) chunks")
                }
            }
        }
    }

    func stopPolling() {
        pollTask?.cancel()
        pollTask = nil
    }

    // MARK: - Position Tracking

    /// Call from the view whenever playback time updates.
    func updatePlaybackPosition(_ currentTime: TimeInterval) {
        latestPlaybackPosition = currentTime

        guard !chunks.isEmpty else {
            if activeChunkIndex != nil {
                activeChunkIndex = nil
            }
            return
        }

        // Chunks are sorted by startTime, but retained partial-overlap rows
        // can have non-monotonic end times. Prefer a final-pass row whenever
        // both passes cover playback, then the latest-starting row within
        // the same pass. Only fall back to the closest preceding row when
        // playback lies in a transcript gap or past the final row.
        var preceding: Int?
        var covering: Int?
        for (index, chunk) in chunks.enumerated() {
            if chunk.startTime <= currentTime {
                // Nested rows make latest-start and closest-preceding
                // different concepts. In a gap, retain the row whose audio
                // ended most recently; use later display order only to break
                // an equal-end tie.
                if let currentPreceding = preceding {
                    if chunk.endTime >= chunks[currentPreceding].endTime {
                        preceding = index
                    }
                } else {
                    preceding = index
                }
                // Transcript audio envelopes are half-open [start, end), so
                // a row ending exactly here must not outrank one that starts
                // at the same boundary.
                if chunk.endTime > currentTime {
                    if let currentCovering = covering {
                        let currentChunk = chunks[currentCovering]
                        let candidateIsFinal =
                            chunk.pass == TranscriptPassType.final_.rawValue
                        let currentIsFinal =
                            currentChunk.pass == TranscriptPassType.final_.rawValue

                        if candidateIsFinal || !currentIsFinal {
                            covering = index
                        }
                    } else {
                        covering = index
                    }
                }
            } else {
                break
            }
        }
        let resolved = covering ?? preceding
        if activeChunkIndex != resolved {
            activeChunkIndex = resolved
        }
    }

    /// Returns true if the given time falls within any known ad window (legacy path).
    func isAdSegment(startTime: Double, endTime: Double) -> Bool {
        adWindows.contains { ad in
            ad.startTime < endTime && ad.endTime > startTime
        }
    }

    /// Returns the highest ad confidence score overlapping this chunk, or nil (legacy path).
    func adConfidence(startTime: Double, endTime: Double) -> Double? {
        let overlapping = adWindows.filter { ad in
            ad.startTime < endTime && ad.endTime > startTime
        }
        return overlapping.map(\.confidence).max()
    }

    /// Returns all Phase 5 decoded spans overlapping the chunk at `chunkIndex`.
    /// Uses the pre-computed mapping built during refresh() for O(1) lookup.
    ///
    /// playhead-d666 R6 — NOT A DISPLAY POPULATION, and as of this round it has
    /// no production caller at all; it survives as the control the tests use to
    /// show this bead narrowed what a row ASSERTS and nothing else. Every one of
    /// the six defects on this bead was a display surface taking `.first` of
    /// this set: the highlight, the badge's span identities, the spoken label,
    /// the popover tap target, and twice more inside the popover's fallback. A
    /// new per-row display consumer belongs on
    /// ``adClaimingSpansOverlapping(chunkIndex:)``.
    func decodedSpansOverlapping(chunkIndex: Int) -> [DecodedSpan] {
        spansByChunkIndex[chunkIndex] ?? []
    }

    /// Whether this chunk should receive ad highlighting (copper bar, AD badge,
    /// background tint). True if the chunk overlaps any DecodedSpan that makes
    /// an ad CLAIM, or any user-marked AdWindow.
    ///
    /// playhead-d666: a span whose only presence anchor is
    /// `.sustainedMusicOffset` does NOT make that claim. `AnchorRef` and
    /// `BackfillEvidenceFusion` both spell it out — the sustained-music
    /// proposer is a TARGETING signal ("an ad likely begins right AFTER this
    /// music"), never a verdict about the audio it covers, which is why
    /// `DecisionMapper` demotes such a span to `.markOnly` and never lets it
    /// auto-skip. This surface honoured none of that: it painted "AD" over the
    /// show's own words on nothing but a music bed. On asset 48E903D7 that was
    /// the clip outro — "to that full episode, I've linked it down below. Check
    /// the description. Thank you." — drawn as an ad because an outro music bed
    /// started 1.5 s into it, and vetoed by the listener as a false positive.
    ///
    /// A user-marked window still wins: that IS a verdict, and the listener's
    /// own. So is any span carrying corroborating presence evidence, music or
    /// not — only the bare hint is silenced.
    func isAdHighlighted(chunkIndex: Int) -> Bool {
        userMarkedChunkIndices.contains(chunkIndex)
            || adClaimingSpansByChunkIndex[chunkIndex] != nil
    }

    /// The decoded spans overlapping this chunk that make an ad CLAIM — i.e.
    /// `decodedSpansOverlapping(chunkIndex:)` minus every span whose only
    /// presence anchor is a sustained-music hint (playhead-d666).
    ///
    /// This is what the AD badge must group by, NOT the full overlap set. The
    /// badge asks "does this row start a span the previous row was not already
    /// under" by comparing span ids; feeding it a music-only id makes a silenced
    /// span join two rows together, so the real span beginning on the second row
    /// looks like a continuation and never gets its badge. The copper bar still
    /// drew, so the symptom is an unlabelled ad region — the one case where
    /// suppressing the hint costs a claim the app is entitled to make.
    func adClaimingSpansOverlapping(chunkIndex: Int) -> [DecodedSpan] {
        adClaimingSpansByChunkIndex[chunkIndex] ?? []
    }

    /// Whether this row carries the "AD" badge: the first row of a decoded ad
    /// span, or the first row of a user-marked ad region.
    ///
    /// playhead-d666 R1: this decision lived inline in `TranscriptPeekView`,
    /// where nothing could test it and where it read the FULL overlap set. Both
    /// halves are fixed together — it now groups by CLAIMING spans, and it is
    /// reachable from a test.
    func showsAdBadge(chunkIndex: Int) -> Bool {
        guard isAdHighlighted(chunkIndex: chunkIndex) else { return false }
        guard chunkIndex > 0 else { return true }
        let claiming = adClaimingSpansOverlapping(chunkIndex: chunkIndex)
        // A decoded claim groups by span identity, so two adjacent but distinct
        // spans each get their own badge.
        if !claiming.isEmpty {
            let previousIds = Set(
                adClaimingSpansOverlapping(chunkIndex: chunkIndex - 1).map(\.id)
            )
            return Set(claiming.map(\.id)).isDisjoint(with: previousIds)
        }
        // User-marked regions carry no span id — badge the first lit row.
        return !isAdHighlighted(chunkIndex: chunkIndex - 1)
    }

    /// The span the tap-to-explain popover opens for on this row.
    ///
    /// playhead-d666 R3 — THE FOURTH CONSUMER, and the same shape as the other
    /// three. The view read `decodedSpansOverlapping(chunkIndex:).first`, and
    /// `fetchDecodedSpans` orders by `startTime`, so on this bead's geometry
    /// ("an ad begins right AFTER this music") the earliest-starting overlap is
    /// the SILENCED span. A row lit and badged by a corroborated span therefore
    /// opened a popover headed "AD SEGMENT / DETECTED FROM: sustained music"
    /// carrying the hint's duration and time range — the same misattribution R2
    /// fixed for VoiceOver, one surface over. Worse than a wrong caption: the
    /// popover's "This isn't an ad" reverts THAT span, so the listener's veto
    /// landed on the hint while the row stayed lit by the claim they were
    /// actually looking at, and the correction — the highest-fidelity signal the
    /// app has — was recorded against the wrong range.
    ///
    /// Measured on `db-corrected2`: 112 transcript rows sit under both a
    /// music-only span and a claiming span, and the music-only span sorts first
    /// on all 112.
    ///
    /// playhead-d666 R6 — AND THERE IS NO FALLBACK. R3 shipped this as
    /// "claiming span, else the first overlapping span", so that a row the app
    /// draws nothing about kept a tap that opened the hint. R4 found a defect
    /// inside that fallback and added a guard; R5 found a defect inside R4's
    /// guard and added a second, and recorded that neither subsumes the other.
    /// Three consecutive rounds, three defects, all in the same two lines, each
    /// one a fresh ad-hoc condition on a predicate with no closure argument.
    ///
    /// The fallback is deleted rather than guarded a third time, for four
    /// reasons that are about what it IS, not about which cells had bitten:
    ///
    ///   • IT MAKES THE CLAIM THIS BEAD EXISTS TO STOP. `AdRegionPopover` hard-
    ///     codes the header "AD SEGMENT" and lists "Sustained music leading
    ///     into this segment" under "DETECTED FROM". Bar, tint, badge and
    ///     spoken label were all moved onto the claiming set precisely so this
    ///     row asserts nothing. Guarding WHEN the popover may assert never
    ///     changed WHAT it asserts.
    ///   • NOBODY CAN FIND IT. On such a row `isAdHighlighted` is false (no
    ///     copper bar, no background tint), `showsAdBadge` is false, and
    ///     `accessibilityLabel` is the bare "\(ts): \(text)" — nothing on
    ///     screen or in VoiceOver says a tap does anything. It is not an
    ///     affordance, it is an undocumented tap target on unmarked text, and
    ///     a VoiceOver listener could never reach it at all.
    ///   • R4 ALREADY WROTE THE ARGUMENT AND APPLIED IT TO HALF THE
    ///     POPULATION: "Returning nil is the honest answer, not a lost
    ///     affordance: the app has no ad claim about that row to explain, and
    ///     un-marking is what the sheet's 'not an ad' marking mode is for."
    ///     That premise is true of the WHOLE fallback, not only of the cell
    ///     that had been measured to hurt someone.
    ///   • THE VETO SURVIVES BY A BETTER ROUTE. `TranscriptPeekView`'s "Not ad"
    ///     header toggle is rendered unconditionally, selects any row painted
    ///     or not, and `submitNotAdChunks` builds a synthetic span over the
    ///     range the USER drew before calling the same `onRevertAdWindows`. The
    ///     fallback's range was the music proposer's: measured on
    ///     `db-corrected2`, over the 347 rows where it still fired the hint
    ///     overhangs the tapped row by a median 11.5 s (max 32.5 s), and on 34
    ///     of them it reaches an `ad_window` in candidate/confirmed/applied
    ///     that does not touch the tapped row at all — six of those
    ///     `dayZeroRediffByteExact`, the highest-certainty auto-skip class.
    ///
    /// WHAT WAS GIVEN UP, stated plainly so it can be restored in one line: an
    /// unpainted row whose only overlapping span is a bare sustained-music hint
    /// no longer opens the tap-to-explain popover. 347 of 94,099
    /// `transcript_chunks` rows on `db-corrected2` (≈174 canonical, the rest
    /// fast/final duplicates) were in that state; every painted row keeps its
    /// popover, and every row keeps "Not ad" mode.
    ///
    /// Deleting it removes both of R4's and R5's guards and the
    /// `liveUserMarkedWindows` mirror they read, and makes the R4/R5/R6 defect
    /// class structurally unreachable rather than individually excluded.
    func popoverSpan(chunkIndex: Int) -> DecodedSpan? {
        adClaimingSpansOverlapping(chunkIndex: chunkIndex).first
    }

    /// What VoiceOver speaks for the row at `chunkIndex`.
    ///
    /// playhead-d666 R2 — WHY THIS COMPOSES HERE RATHER THAN IN THE VIEW.
    /// The spoken label names a SPAN: its duration and the evidence it was
    /// detected from. R1 gated the whole decoded branch on a per-ROW boolean
    /// (`makesAdClaim`) but left the view choosing that span out of the FULL
    /// overlap set, and `fetchDecodedSpans` orders by `startTime`, so `.first`
    /// is the EARLIEST-starting overlapping span. The sustained-music proposer
    /// produces exactly the geometry where that is the silenced one — a music
    /// run, then the ad it is pointing at — so a row under both was announced
    /// as "Ad segment, 12 seconds, detected from sustained music" while the
    /// claim it is actually drawn for was a 60-second corroborated post-roll.
    /// The row's ad-ness was right; its length and its whole reason were the
    /// hint's, which is the one thing this bead established may never speak for
    /// an ad.
    ///
    /// Composing it here — not in the view — is what makes that assertable:
    /// `TranscriptPeekView`'s `@State private` storage makes its memberwise
    /// initializer private, so a test cannot construct one and "does the view
    /// hand the label the right population?" was unaskable. It is the same move
    /// R1 made for `showsAdBadge`, for the same reason.
    ///
    /// Out of range returns the empty string: a row that does not exist has
    /// nothing to say, and a trap here would crash the reader over a stale
    /// index rather than a real defect.
    func accessibilityLabel(chunkIndex: Int) -> String {
        guard chunks.indices.contains(chunkIndex) else { return "" }
        let chunk = chunks[chunkIndex]
        return TranscriptRowAccessibility.label(
            chunk: chunk,
            isAd: isAdSegment(startTime: chunk.startTime, endTime: chunk.endTime),
            claimingSpans: adClaimingSpansOverlapping(chunkIndex: chunkIndex)
        )
    }

    // playhead-d666 R6: `decodedSpansOverlapping(startTime:endTime:)` is
    // deleted. It was documented as "retained for callers that don't have a
    // chunk index handy" and had none — zero production callers, one test — and
    // it vended the UNFILTERED span set with no claim filter, which is the
    // exact shape of the six defects this bead has now produced (a display-side
    // consumer taking `.first` of a population it is not entitled to make an ad
    // claim over). `decodedSpans` is still `private(set)` and readable for the
    // structural/persistence reads that genuinely want every row.

    /// Resolve currently visible audio envelopes captured from displayed rows
    /// to the combined range used by the mark-ad and not-ad submission paths.
    /// Capturing the envelope at tap time prevents a later polling refresh
    /// from redirecting an array offset to unrelated audio; intersecting with
    /// the latest projection closes the window between SwiftUI's deferred
    /// selection reconciliation and a confirmation action.
    func selectedChunkTimeRange(
        selections: Set<TranscriptChunkSelection>
    ) -> (start: TimeInterval, end: TimeInterval)? {
        let visibleSelections = reconciledSelections(selections)
        guard let start = visibleSelections.map(\.startTime).min(),
              let end = visibleSelections.map(\.endTime).max()
        else {
            return nil
        }
        return (start: start, end: end)
    }

    /// Drop captured envelopes that no longer correspond to any visible
    /// canonical row. Exact-span fast→final replacement remains selected,
    /// while a differently bounded replacement cannot become an invisible,
    /// impossible-to-deselect mark or veto.
    func reconciledSelections(
        _ selections: Set<TranscriptChunkSelection>
    ) -> Set<TranscriptChunkSelection> {
        let visibleSelections = Set(chunks.map(TranscriptChunkSelection.init))
        return selections.intersection(visibleSelections)
    }

    // MARK: - Untranscribed-tail mark (playhead-m1l9)

    /// Shortest tail span we will offer to mark — avoids degenerate sub-2s
    /// marks when the playhead is a hair from the episode end.
    private static let minTailMarkWidth: TimeInterval = 2.0

    /// Longest remaining-to-end span we treat as a "post-roll to the end".
    /// Beyond this the playhead isn't plausibly inside a post-roll and a
    /// mark-to-end would over-mark a large untranscribed region; the player
    /// "Hearing an ad" button (a bounded ±window seed) is the right tool
    /// there. Generous enough to cover long ad pods (5 minutes).
    private static let maxTailMarkWidth: TimeInterval = 300.0

    /// The furthest transcript time the chunk-selection mark flow can reach:
    /// the max chunk end. That flow assembles a span by tapping rows, so it
    /// stops where the rows stop — this is TRANSCRIPT EVIDENCE ("how far did
    /// we find speech?"), not scan reach.
    /// With no chunks at all this is 0 — the same sentinel the pre-7tn8 gate
    /// used, so a playhead at exactly 0.0 on a transcript-less episode keeps
    /// its old (no-affordance) behavior.
    var lastTranscribedTime: TimeInterval {
        chunks.lazy.map(\.endTime).max() ?? 0
    }

    /// The furthest time the fast pass has SCANNED ("how far did we look?"):
    /// the evidence end unioned with the coverage watermark. The watermark can
    /// legitimately run past the last chunk, because a shard that yields zero
    /// segments still advances coverage (`TranscriptEngineService`) — we
    /// looked at that audio and found no speech in it.
    ///
    /// playhead-7tn8: this is deliberately NOT what gates the tail-mark
    /// affordance any more. It stays because the scan question is real and
    /// distinct — it is what separates a transient not-yet-analyzed tail from
    /// a genuine silent blind spot in ``tailCoverage(at:)``.
    var lastCoveredTime: TimeInterval {
        max(lastTranscribedTime, fastTranscriptCoverageEndTime ?? 0)
    }

    /// What is known about the audio under a given playback time.
    ///
    /// Coverage answers two different questions and playhead-7tn8 turns on
    /// keeping them apart: the watermark answers "did we LOOK here?", the
    /// chunks answer "did we FIND SPEECH here?".
    enum TailCoverage: Equatable, Sendable {
        /// Transcript rows reach this time — the chunk-selection "Mark ad"
        /// flow works, so the tail affordance is not needed.
        case transcribed
        /// Inside the scan watermark but past the last chunk: we looked and
        /// found no speech. A music-bedded ad produces exactly this, and no
        /// deterministic or lexical signal can bite on it — the listener is
        /// the only witness, so the affordance must be offered.
        case scannedWithoutSpeech
        /// Past the scan watermark: analysis has not reached here yet. Kept
        /// distinct from ``scannedWithoutSpeech`` — this one is transient and
        /// segments may still arrive.
        case notYetScanned
    }

    /// Classify `time` against the two coverage questions. See ``TailCoverage``.
    func tailCoverage(at time: TimeInterval) -> TailCoverage {
        // Both boundaries are INCLUSIVE of their region: a time landing exactly
        // on the last chunk end is still `.transcribed`, and one landing
        // exactly on the watermark is still `.scannedWithoutSpeech`. That
        // reproduces the pre-7tn8 `currentTime > lastCoveredTime` gate — a
        // playhead sitting on the edge is not yet in the tail.
        guard time > lastTranscribedTime else { return .transcribed }
        return time <= lastCoveredTime ? .scannedWithoutSpeech : .notYetScanned
    }

    /// A coverage-FREE ad-mark span for the untranscribed tail, or nil when
    /// the tail affordance does not apply.
    ///
    /// The chunk-selection "Mark ad" flow can only build a mark from existing
    /// transcript chunks, which extend only to `lastTranscribedTime`. When a
    /// post-roll runs past the last chunk to the episode end, there are no
    /// chunks to tap and the ad is unmarkable (playhead-m1l9). This returns
    /// `[currentTime, episodeDuration]` so the untranscribed tail can be
    /// marked without any chunks — routed through the same coverage-free
    /// `injectUserMarkedAd` path the player "Hearing an ad" button uses.
    ///
    /// playhead-7tn8: the gate keys on TRANSCRIPT EVIDENCE, not scan reach.
    /// Keying it on `lastCoveredTime` suppressed the affordance in exactly the
    /// case it exists for — a music-bedded post-roll, where the shards were
    /// scanned, produced zero segments, advanced the watermark past the last
    /// chunk, and left the listener as the only witness that the ad happened.
    /// A scanned-but-silent stretch is often just music, so the affordance can
    /// appear over non-ad audio; that is accepted deliberately, because it is
    /// OPT-IN and only fires when someone actually heard an ad. Weight the
    /// resulting correction through the existing certainty tiering rather than
    /// suppressing the report.
    ///
    /// Returns nil unless the playhead sits PAST the transcript evidence
    /// (otherwise the ordinary chunk-selection flow already works) AND the
    /// remaining span to the episode end is post-roll-sized (a meaningful
    /// span that isn't a large over-mark). The bounded-window player button
    /// handles the "hearing an ad far from the end" case.
    func untranscribedTailMarkSpan(
        currentTime: TimeInterval,
        episodeDuration: TimeInterval
    ) -> (start: Double, end: Double)? {
        guard episodeDuration > 0 else { return nil }
        // The playhead must be in the untranscribed tail — territory the
        // chunk-selection flow cannot reach. Both uncovered states qualify:
        // `.scannedWithoutSpeech` is the silent-shard blind spot this bead
        // reopened, `.notYetScanned` is m1l9's original post-roll case.
        guard tailCoverage(at: currentTime) != .transcribed else { return nil }
        // A post-roll-sized span must remain to the episode end.
        let remaining = episodeDuration - currentTime
        guard remaining >= Self.minTailMarkWidth,
              remaining <= Self.maxTailMarkWidth else { return nil }
        return (start: currentTime, end: episodeDuration)
    }

    /// Debug stats summary for TestFlight diagnostics.
    private(set) var debugStats: String = "loading…"

    private func updateDebugStats(snapshot: TranscriptPeekSnapshot) {
        let count = chunks.count
        let fmt = { (t: Double) -> String in
            let m = Int(t) / 60
            let s = Int(t) % 60
            return String(format: "%d:%02d", m, s)
        }

        var parts: [String] = []

        // Chunk count + time range
        if count > 0 {
            let minTime = chunks.first?.startTime ?? 0
            let maxTime = chunks.map(\.endTime).max() ?? 0
            parts.append("\(count) chunks \(fmt(minTime))–\(fmt(maxTime))")
        } else {
            parts.append("0 chunks")
        }

        // Ad window count
        parts.append("\(adWindows.count) ads")

        // Raw chunk count (before display canonicalization) to detect whether
        // writes are missing or overlapping rows were projected away.
        if snapshot.rawChunkCount != count {
            parts.append("raw \(snapshot.rawChunkCount)")
        }

        // Asset coverage watermarks + session state
        if let featCov = snapshot.featureCoverageEnd {
            parts.append("feat \(fmt(featCov))")
        }
        if let txCov = snapshot.fastTranscriptCoverageEnd {
            parts.append("tx \(fmt(txCov))")
        }
        if let session = snapshot.latestSessionState {
            parts.append(session)
        }
        if snapshot.fetchFailed {
            parts.append("err")
        }

        // Streaming decode diagnostics.
#if DEBUG
        let seed = UserDefaults.standard.integer(forKey: "debug_streamingSeeded")
        let streamingChunks = UserDefaults.standard.integer(forKey: "debug_streamingChunks")
        let strShards = UserDefaults.standard.integer(forKey: "debug_streamingShards")
        parts.append("s:\(seed/1024)k c:\(streamingChunks) sh:\(strShards)")
#endif

        debugStats = parts.joined(separator: " · ")
    }

    // MARK: - Private

    /// Rebuild the chunk-index → overlapping-spans lookup table and
    /// the user-marked chunk index set.
    /// Called once per refresh cycle so per-row view queries are O(1).
    private func rebuildSpansByChunkIndex() {
        var mapping: [Int: [DecodedSpan]] = [:]
        var userMarked = Set<Int>()
        var claiming: [Int: [DecodedSpan]] = [:]

        // playhead-u45d: the same defect one field over. This set used to
        // filter on `boundaryState` alone, so vetoing an ad the listener had
        // marked themselves flipped `decisionState` to `reverted` and left the
        // rows lit — the write landing where nothing looked, exactly as with
        // decoded spans. `reverted` IS the durable record the veto writes;
        // reading it is the correction taking effect, not the view hiding a
        // row it was never told about.
        let userMarkedWindows = adWindows.filter {
            $0.boundaryState == "userMarked"
                && $0.decisionState != AdDecisionState.reverted.rawValue
        }

        for (idx, chunk) in chunks.enumerated() {
            let overlapping = decodedSpans.filter { span in
                span.startTime < chunk.endTime && span.endTime > chunk.startTime
            }
            if !overlapping.isEmpty {
                mapping[idx] = overlapping
                // playhead-d666: filtered PER SPAN, not collapsed to a per-row
                // verdict. One corroborated span overlapping this row is an ad
                // claim about this row, and a bare music hint sitting on top of
                // it cannot retract that — so the survivors, not merely their
                // count, are what the row is entitled to assert.
                let claims = overlapping.filter {
                    !$0.anchorProvenance.carriesOnlyMusicPresenceHint
                }
                if !claims.isEmpty {
                    claiming[idx] = claims
                }
            }

            if userMarkedWindows.contains(where: { ad in
                ad.startTime < chunk.endTime && ad.endTime > chunk.startTime
            }) {
                userMarked.insert(idx)
            }
        }
        spansByChunkIndex = mapping
        userMarkedChunkIndices = userMarked
        adClaimingSpansByChunkIndex = claiming
    }

    /// Pull a fresh snapshot and apply it to observable state. Internal (not
    /// private) so tests can drive one deterministic load without racing the
    /// `startPolling` Task.
    func refresh() async {
        let snapshot = await dataSource.fetchSnapshot(assetId: analysisAssetId)
        // A cancelled polling generation may finish after a replacement task
        // because data sources are not required to cooperate with
        // cancellation. Never let that stale response overwrite newer state.
        guard !Task.isCancelled else { return }

        if snapshot.fetchFailed {
            logger.error("Transcript peek: snapshot fetch reported failure for asset \(self.analysisAssetId)")
            // Poll failures are not authoritative empty projections. Retain
            // the last good rows so active playback and in-progress mark/veto
            // selections survive a transient store error.
            updateDebugStats(snapshot: snapshot)
            return
        }
        chunks = snapshot.chunks
        if let latestPlaybackPosition {
            updatePlaybackPosition(latestPlaybackPosition)
        } else {
            activeChunkIndex = nil
        }
        adWindows = snapshot.adWindows
        decodedSpans = snapshot.decodedSpans
        fastTranscriptCoverageEndTime = snapshot.fastTranscriptCoverageEnd
        rebuildSpansByChunkIndex()
        updateDebugStats(snapshot: snapshot)
    }
}
