// TrainingExampleMaterializer.swift
// playhead-4my.10.1: snapshots the per-region evidence + decision +
// correction ledger for a single asset into the durable
// `training_examples` table.
//
// Invocation contract: called once per BackfillJob, after the runner
// finishes a phase. Within one BackfillJob the cohort is stable, so a
// single materialization pass captures a coherent snapshot. Subsequent
// cohort prunes (`AnalysisStore.pruneOrphanedScansForCurrentCohort`)
// reap the source ledger rows but leave the materialized examples
// untouched — that's the durability win this bead is about.
//
// Spine: `semantic_scan_results`. One training example per scan-result
// row, because every scan-result row carries the
// (startAtomOrdinal, endAtomOrdinal, transcriptVersion, transcriptQuality,
//  scanCohortJSON) tuple the spec requires. Decision + evidence +
// correction ledgers are joined in by interval-overlap.
//
// Idempotency / cohort-durability: `replaceTrainingExamples(forAsset:with:)`
// performs a per-row id-keyed upsert (`INSERT OR REPLACE`). Each example's
// id is deterministic (`"te-\(scan.id)"`), so re-running the materializer
// over the same spine overwrites the matching rows in place. Crucially,
// rows previously materialized under a *different* scan-result spine (e.g.
// an earlier cohort whose spine was pruned) survive — they remain useful
// training data for downstream evaluation regardless of the current
// cohort, which is the durability guarantee this bead delivers.

import CryptoKit
import Foundation

struct TrainingExampleMaterializer: Sendable {

    init() {}

    /// Materializes training examples for a single asset by joining the
    /// scan-result spine with overlapping evidence, decision, and
    /// correction events.
    func materialize(
        forAsset analysisAssetId: String,
        store: AnalysisStore,
        now: Double = Date().timeIntervalSince1970
    ) async throws {
        let scanResults = try await store.fetchSemanticScanResults(
            analysisAssetId: analysisAssetId,
            scanPass: nil
        )
        guard !scanResults.isEmpty else {
            // Nothing to materialize. Leave any pre-existing rows alone —
            // they may have come from an earlier successful run that we
            // don't want to nuke just because the current cohort prune
            // emptied the spine.
            return
        }

        let evidenceEvents = try await store.fetchEvidenceEvents(
            analysisAssetId: analysisAssetId
        )
        let decisionEvents = try await store.loadDecisionEvents(
            for: analysisAssetId
        )
        let correctionEvents = try await store.loadCorrectionEvents(
            analysisAssetId: analysisAssetId
        )
        let adWindows = try await store.fetchAdWindows(
            assetId: analysisAssetId
        )

        // Pre-decode evidence ordinal arrays once (cheap to do up front
        // and avoids re-parsing per scan-row in the inner loop).
        let prepared: [PreparedEvidence] = evidenceEvents.compactMap { ev in
            guard let data = ev.atomOrdinals.data(using: .utf8),
                  let ordinals = try? JSONDecoder().decode([Int].self, from: data),
                  let firstOrdinal = ordinals.min(),
                  let lastOrdinal = ordinals.max()
            else { return nil }
            let certainty = Self.parseCertainty(ev.evidenceJSON)
            return PreparedEvidence(
                event: ev,
                firstOrdinal: firstOrdinal,
                lastOrdinal: lastOrdinal,
                certainty: certainty
            )
        }

        var examples: [TrainingExample] = []
        examples.reserveCapacity(scanResults.count)

        for scan in scanResults {
            // M5: Filter the spine to successful scans. Failed/refusal/
            // decoding-failure rows carry no usable signals, so they would
            // produce empty/garbage training examples that pollute the
            // corpus.
            guard scan.status == .success else { continue }

            let evidenceForScan = prepared.filter {
                Self.intervalOverlaps(
                    aFirst: $0.firstOrdinal, aLast: $0.lastOrdinal,
                    bFirst: scan.windowFirstAtomOrdinal,
                    bLast: scan.windowLastAtomOrdinal
                )
            }
            // Decision events carry an `AdWindow.id`, not the scan-row id.
            // We approximate the join: for each scan, find the
            // AdWindows that overlap the scan in *time*, then pick
            // decision events whose `windowId` matches one of those.
            // Falls back to nil when no overlapping window exists, so
            // editorial regions don't inherit a paid-ad decision.
            let overlappingAdWindows = adWindows.filter { aw in
                Self.timeIntervalOverlaps(
                    aStart: aw.startTime, aEnd: aw.endTime,
                    bStart: scan.windowStartTime,
                    bEnd: scan.windowEndTime
                )
            }
            let overlappingWindowIds: Set<String> = Set(
                overlappingAdWindows.map { $0.id }
            )
            let decisionForScan = Self.bestDecision(
                for: scan,
                in: decisionEvents,
                overlappingWindowIds: overlappingWindowIds
            )
            // M2: actual skip execution comes from `AdWindow.wasSkipped`,
            // not from the policy's eligibility gate. Eligible-but-not-
            // executed windows must NOT be labelled as skipped.
            let scanWasSkipped = overlappingAdWindows.contains { $0.wasSkipped }
            let corrections = correctionEvents.filter {
                Self.correctionOverlaps(
                    correctionScope: $0.scope,
                    scanStart: scan.windowStartTime,
                    scanEnd: scan.windowEndTime,
                    scanFirstOrdinal: scan.windowFirstAtomOrdinal,
                    scanLastOrdinal: scan.windowLastAtomOrdinal
                )
            }

            let example = makeExample(
                scan: scan,
                evidence: evidenceForScan,
                decision: decisionForScan,
                corrections: corrections,
                wasSkipped: scanWasSkipped,
                now: now
            )
            examples.append(example)
        }

        try await store.replaceTrainingExamples(
            forAsset: analysisAssetId,
            with: examples
        )
    }

    // MARK: - Per-scan example builder

    private func makeExample(
        scan: SemanticScanResult,
        evidence: [PreparedEvidence],
        decision: DecisionEvent?,
        corrections: [CorrectionEvent],
        wasSkipped: Bool,
        now: Double
    ) -> TrainingExample {
        let fmEvidence = evidence.filter { $0.event.sourceType == .fm }
        let lexEvidence = evidence.filter { $0.event.sourceType == .lexical }

        let fmPositive = !fmEvidence.isEmpty || scan.disposition == .containsAd
        let fmCertainty = fmEvidence.map { $0.certainty }.max() ?? 0.0
        // M1: split "lexical fired" from "lexical positive". The lexical
        // scanner only persists evidence when it produces a positive
        // finding (it does not write rows for "no hit"). So
        // `lexicalFired = !lexEvidence.isEmpty` means "the lexicon
        // matched"; absence is silence, not negative testimony. The
        // bucketer needs both signals to distinguish a true
        // lexical-vs-FM disagreement from a normal "lexicon was quiet"
        // case.
        let lexicalFired = !lexEvidence.isEmpty
        let lexicalPositive = lexicalFired
        let classifierConfidence = decision?.skipConfidence ?? 0.0
        let decisionWasSkipEligible =
            decision?.policyAction == "autoSkipEligible" ||
            decision?.eligibilityGate == "eligible"

        let userReverted = corrections.contains {
            $0.source?.kind == .falsePositive
        }
        let userReportedFalseNegative = corrections.contains {
            $0.source?.kind == .falseNegative
        }

        let signals = TrainingExampleBucketerSignals(
            fmPositive: fmPositive,
            fmCertainty: fmCertainty,
            lexicalFired: lexicalFired,
            lexicalPositive: lexicalPositive,
            classifierConfidence: classifierConfidence,
            decisionWasSkipEligible: decisionWasSkipEligible,
            userReverted: userReverted,
            userReportedFalseNegative: userReportedFalseNegative,
            transcriptQuality: scan.transcriptQuality.rawValue
        )
        let bucket = TrainingExampleBucketer.bucket(for: signals)

        let evidenceSourceNames = Self.distinctSourceNames(from: evidence)

        // M2: only label the example as `"skipped"` when the policy
        // actually executed a skip. Eligibility-without-execution is
        // recorded as `"eligibleNotSkipped"` so the corpus distinguishes
        // policy intent from playback-time outcome. We do not invent a
        // `"skipped"` label on eligibility alone — that would mislabel
        // every banner the user ignored as a successful skip.
        let userAction: String? = {
            if userReverted { return "reverted" }
            if userReportedFalseNegative { return "reportedAd" }
            if wasSkipped { return "skipped" }
            if decisionWasSkipEligible { return "eligibleNotSkipped" }
            return nil
        }()

        // commercialIntent / ownership: not directly carried on the scan
        // row, but we can infer a coarse value from disposition + bucket.
        // Phase-10.x will replace this with the FM refinement output once
        // it's persisted in a queryable shape.
        let commercialIntent = Self.inferCommercialIntent(
            bucket: bucket, fmPositive: fmPositive
        )
        let ownership = Self.inferOwnership(
            bucket: bucket, fmPositive: fmPositive
        )

        let textSnapshotHash = Self.stableHash(
            assetId: scan.analysisAssetId,
            firstOrdinal: scan.windowFirstAtomOrdinal,
            lastOrdinal: scan.windowLastAtomOrdinal,
            transcriptVersion: scan.transcriptVersion
        )

        let id = "te-\(scan.id)"

        return TrainingExample(
            id: id,
            analysisAssetId: scan.analysisAssetId,
            startAtomOrdinal: scan.windowFirstAtomOrdinal,
            endAtomOrdinal: scan.windowLastAtomOrdinal,
            transcriptVersion: scan.transcriptVersion,
            startTime: scan.windowStartTime,
            endTime: scan.windowEndTime,
            textSnapshotHash: textSnapshotHash,
            // We don't durably retain the verbatim transcript snapshot
            // here — the source `transcript_chunks` rows already have
            // it, and copying is expensive. Future bead may opt-in to
            // retain on a per-bucket basis. Hash is always present.
            textSnapshot: nil,
            bucket: bucket,
            commercialIntent: commercialIntent,
            ownership: ownership,
            evidenceSources: evidenceSourceNames,
            fmCertainty: fmCertainty,
            classifierConfidence: classifierConfidence,
            userAction: userAction,
            eligibilityGate: decision?.eligibilityGate,
            scanCohortJSON: scan.scanCohortJSON,
            // L4: emit `nil` when no decision overlapped this scan.
            // Empty string was indistinguishable from a buggy serializer.
            decisionCohortJSON: decision?.decisionCohortJSON,
            transcriptQuality: scan.transcriptQuality.rawValue,
            createdAt: now
        )
    }

    // MARK: - Helpers

    private struct PreparedEvidence {
        let event: EvidenceEvent
        let firstOrdinal: Int
        let lastOrdinal: Int
        let certainty: Double
    }

    /// Closed-interval overlap on atom-ordinal ranges.
    private static func intervalOverlaps(
        aFirst: Int, aLast: Int,
        bFirst: Int, bLast: Int
    ) -> Bool {
        aFirst <= bLast && bFirst <= aLast
    }

    /// Half-open (a.start ≤ t < a.end) overlap on `Double` time ranges.
    /// Touching endpoints (e.g. one window ends at 10s and the next
    /// starts at 10s) do *not* count as overlap — that's the standard
    /// audio-domain convention and avoids two adjacent ad-windows both
    /// claiming the same scan region during the join.
    private static func timeIntervalOverlaps(
        aStart: Double, aEnd: Double,
        bStart: Double, bEnd: Double
    ) -> Bool {
        aStart < bEnd && bStart < aEnd
    }

    /// Correction scopes are serialized via `CorrectionScope.serialized`.
    /// The two span-bound forms have *different* coordinate systems:
    ///
    ///   * `.exactSpan`        — `"exactSpan:<assetId>:<lowerOrdinal>:<upperOrdinal>"`
    ///                           (atom **ordinals**, integers)
    ///   * `.exactTimeSpan`    — `"exactTimeSpan:<assetId>:<startTime>:<endTime>"`
    ///                           (seconds, doubles)
    ///
    /// For span-bound scopes we route through `CorrectionScope.deserialize(_:)`
    /// so the canonical parser handles asset IDs that contain colons and the
    /// fixed-precision time format. We then compare each scope to the *right*
    /// scan field: ordinals to `scan.windowFirstAtomOrdinal/windowLastAtomOrdinal`,
    /// times to `scan.windowStartTime/windowEndTime`.
    ///
    /// Wider scopes (`sponsorOnShow`, `phraseOnShow`, `campaignOnShow`,
    /// `domainOwnershipOnShow`, `jingleOnShow`) are not span-bound and match
    /// every scan region of the asset.
    private static func correctionOverlaps(
        correctionScope: String,
        scanStart: Double,
        scanEnd: Double,
        scanFirstOrdinal: Int,
        scanLastOrdinal: Int
    ) -> Bool {
        guard let parsed = CorrectionScope.deserialize(correctionScope) else {
            // Unrecognized prefix: fail closed. A correction we can't
            // localize must not contaminate every scan with userReverted.
            return false
        }
        switch parsed {
        case .exactSpan(_, let ordinalRange):
            return intervalOverlaps(
                aFirst: ordinalRange.lowerBound,
                aLast: ordinalRange.upperBound,
                bFirst: scanFirstOrdinal,
                bLast: scanLastOrdinal
            )
        case .exactTimeSpan(_, let cStart, let cEnd):
            // Closed-interval overlap on time ranges. (We use a closed
            // comparison here, not the half-open `timeIntervalOverlaps`,
            // because correction time spans are user-supplied and may share
            // an endpoint with an adjacent scan; we'd rather over-attribute
            // than miss.)
            return cStart <= scanEnd && scanStart <= cEnd
        case .sponsorOnShow, .phraseOnShow, .campaignOnShow,
             .domainOwnershipOnShow, .jingleOnShow:
            // Wider, non-span-bound scopes. They apply to every region of
            // the asset by construction.
            return true
        }
    }

    /// Picks the strongest decision whose `windowId` matches one of the
    /// `AdWindow` rows that overlap this scan in time. Decision events
    /// carry an `AdWindow.id` rather than the `semantic_scan_results`
    /// row id, so we use the `AdWindow.startTime/endTime` overlap as
    /// the join. When no AdWindow overlaps (e.g. an editorial region
    /// the model rejected), returns `nil` rather than inheriting a
    /// neighbouring window's decision — that would mislabel negatives
    /// as positives.
    private static func bestDecision(
        for scan: SemanticScanResult,
        in events: [DecisionEvent],
        overlappingWindowIds: Set<String>
    ) -> DecisionEvent? {
        guard !overlappingWindowIds.isEmpty else { return nil }
        let candidates = events.filter {
            overlappingWindowIds.contains($0.windowId)
        }
        return candidates.max { $0.skipConfidence < $1.skipConfidence }
    }

    /// Parses the `certainty` field out of a persisted `EvidencePayload`-style
    /// JSON blob. The on-disk shape stores the value as a `CertaintyBand` raw
    /// string (`"weak"` | `"moderate"` | `"strong"`) — see
    /// `BackfillJobRunner.EvidencePayload.certainty`. We map each band to a
    /// representative midpoint so downstream gates (e.g. the bucketer's
    /// `fmCertainty >= 0.7` positive gate) operate against the correct
    /// magnitude. Numeric forms are still accepted for forward/test
    /// compatibility — useful when ad-hoc fixtures or future producers stamp
    /// a numeric certainty directly.
    static func parseCertainty(_ json: String) -> Double {
        guard let data = json.data(using: .utf8),
              let dict = try? JSONSerialization.jsonObject(with: data) as? [String: Any]
        else { return 0.0 }
        // Preferred shape: string band.
        if let band = dict["certainty"] as? String,
           let value = certaintyBandToDouble(band) {
            return value
        }
        // Fallback: numeric form (Double or Int) — fixtures, future shapes.
        if let v = dict["certainty"] as? Double { return v }
        if let v = dict["certainty"] as? Int { return Double(v) }
        return 0.0
    }

    /// Maps a `CertaintyBand` raw string to a representative double.
    ///
    /// Midpoints chosen so that:
    ///   * `weak`     → 0.3 (clearly below the bucketer's 0.5 evidence floor)
    ///   * `moderate` → 0.6 (above the 0.5 floor but below the 0.7 positive gate)
    ///   * `strong`   → 0.9 (well above the 0.7 positive gate)
    ///
    /// Returns `nil` for unknown raw values so callers can distinguish
    /// "string but unrecognized" from "numeric form" if they care.
    static func certaintyBandToDouble(_ raw: String) -> Double? {
        switch raw {
        case "weak":     return 0.3
        case "moderate": return 0.6
        case "strong":   return 0.9
        default:         return nil
        }
    }

    private static func distinctSourceNames(from evidence: [PreparedEvidence]) -> [String] {
        var seen = Set<String>()
        var ordered: [String] = []
        for ev in evidence {
            let name = ev.event.sourceType.rawValue
            if seen.insert(name).inserted {
                ordered.append(name)
            }
        }
        return ordered
    }

    /// Coarse `commercialIntent` inference from bucket label only. The
    /// `fmPositive` parameter is preserved on the signature for symmetry
    /// with `inferOwnership` — and for any future caller that wants to
    /// distinguish positive-but-uncertain from negative-but-uncertain —
    /// but is intentionally unused: without proper domain confidence we
    /// cannot honestly differentiate `.paid` from `.unknown` on the
    /// uncertain/disagreement legs, and a ternary that returns the same
    /// value on both arms is dead code (L1).
    private static func inferCommercialIntent(
        bucket: TrainingExampleBucket,
        fmPositive _: Bool
    ) -> String {
        switch bucket {
        case .positive:
            return CommercialIntent.paid.rawValue
        case .negative:
            return CommercialIntent.organic.rawValue
        case .uncertain, .disagreement:
            return CommercialIntent.unknown.rawValue
        }
    }

    private static func inferOwnership(
        bucket: TrainingExampleBucket,
        fmPositive: Bool
    ) -> String {
        switch bucket {
        case .positive:
            return AdOwnership.thirdParty.rawValue
        default:
            return AdOwnership.unknown.rawValue
        }
    }

    private static func stableHash(
        assetId: String,
        firstOrdinal: Int,
        lastOrdinal: Int,
        transcriptVersion: String
    ) -> String {
        let canonical = "\(assetId)|\(firstOrdinal)|\(lastOrdinal)|\(transcriptVersion)"
        let digest = SHA256.hash(data: Data(canonical.utf8))
        return digest.map { String(format: "%02x", $0) }.joined()
    }
}
