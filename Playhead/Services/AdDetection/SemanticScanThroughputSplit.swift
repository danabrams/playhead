import Foundation

/// playhead-hx6n: the three answers the question "was this FM work done in the
/// foreground or the background?" can honestly have.
///
/// **`.unattributed` is a first-class answer, not a rounding error.** It is the
/// whole reason this type exists. Two separate investigations — playhead-kvs8's
/// re-split of the 2.4× slower-than-realtime FM figure, and the 2026-07-31/08-01
/// stall timeline — ended in "cannot be determined from device data" because
/// `semantic_scan_results` recorded nothing that could attribute a row. Rows
/// written before schema V42 are still in that state and always will be: they
/// are genuinely unattributable, no backfill can honestly repair them, and the
/// only correct thing to do with them is to COUNT THEM SEPARATELY so a reader
/// can see how much of the corpus the split actually covers before believing it.
///
/// A consumer that silently folded them into `.foreground` would reproduce the
/// original defect with a confident face on it.
enum ScanAttributionBucket: String, Sendable, Hashable, CaseIterable {
    case foreground
    case background
    case unattributed
}

extension ScanScenePhase {

    /// Which side of the foreground/background question this recorded phase
    /// answers.
    ///
    /// **`.inactive` is FOREGROUND, and that is a judgment worth stating.** iOS
    /// reports `inactive` for a foreground app that is not currently receiving
    /// events — mid app-switcher, during a system alert, across a phone call
    /// banner. For the question this bucket exists to answer (does FM inference
    /// run at full speed, or is it being throttled by the background execution
    /// regime?) an `inactive` process is still a foreground process with a
    /// foreground CPU allocation, so folding it in with `.active` is the honest
    /// reading. Any consumer that disagrees can read
    /// ``SemanticScanThroughputSplit/byScenePhase``, which keeps all four raw
    /// phases un-rolled-up precisely so this mapping is auditable rather than
    /// baked in irreversibly.
    ///
    /// **`.unknown` is NOT foreground.** It is a recorded non-answer — the
    /// platform was asked and declined — so it buckets as `.unattributed`
    /// alongside a `nil` phase. Mapping it to either side would invent the
    /// attribution the row explicitly says it does not have.
    var attributionBucket: ScanAttributionBucket {
        switch self {
        case .active, .inactive: return .foreground
        case .background:        return .background
        case .unknown:           return .unattributed
        }
    }
}

/// One side of the split: how much audio was screened, and what it cost.
struct ScanThroughputBucket: Sendable, Equatable {
    /// Number of eligible scan rows folded into this bucket.
    var scanCount: Int = 0
    /// Total transcript audio the scans in this bucket covered, in seconds.
    var audioSeconds: Double = 0
    /// Total measured model latency for those scans, in seconds.
    var wallSeconds: Double = 0

    /// Wall seconds spent per audio second. `> 1` is slower than realtime.
    ///
    /// **`nil`, never `1.0`, when there is no audio in the bucket.** A ratio
    /// with a zero denominator is not "realtime"; it is no measurement at all,
    /// and a `1.0` sitting where a missing number belongs is the shape that has
    /// already misled this project more than once (see
    /// `feedback_ask_what_the_quantity_measures_2026-07-29` — a 1.0197 clamped
    /// to 1.0 lit a completeness checkmark on 14.8 % of the audio).
    var realtimeRatio: Double? {
        guard audioSeconds > 0 else { return nil }
        return wallSeconds / audioSeconds
    }

    mutating func add(audioSeconds: Double, wallSeconds: Double) {
        scanCount += 1
        self.audioSeconds += audioSeconds
        self.wallSeconds += wallSeconds
    }

    /// Fold a pre-aggregated group in. Used by the SQL side, where SQLite has
    /// already done the `GROUP BY` and hands back one row per phase — including
    /// the `NULL` phase group, and the `.unknown` group, which both have to land
    /// in the same `.unattributed` bucket without being summed twice.
    mutating func merge(_ other: ScanThroughputBucket) {
        scanCount += other.scanCount
        audioSeconds += other.audioSeconds
        wallSeconds += other.wallSeconds
    }
}

/// playhead-hx6n: FM scan throughput split by the scene phase the scan
/// completed in — the measurement playhead-kvs8 was asked for and could not
/// make, because the attribution did not exist at write time.
///
/// The SQL equivalent, which
/// ``AnalysisStore/fetchSemanticScanThroughputSplit()`` runs and which
/// `docs/investigations/playhead-hx6n-scan-attribution.md` documents for
/// hand-analysis of a device pull, computes exactly these numbers. The two are
/// cross-checked against one shared fixture in
/// `SemanticScanRunAttributionTests` so the documented query cannot drift away
/// from the shipped consumer.
struct SemanticScanThroughputSplit: Sendable, Equatable {
    var foreground: ScanThroughputBucket
    var background: ScanThroughputBucket
    /// Rows whose phase is `nil` (pre-V42) or `.unknown` (recorded non-answer).
    var unattributed: ScanThroughputBucket
    /// Every RECORDED phase, un-rolled-up, so the `.inactive → foreground`
    /// judgment in ``ScanScenePhase/attributionBucket`` stays auditable. Rows
    /// with a `nil` phase appear in ``unattributed`` and in no key here —
    /// there is no scene phase to key them by, and inventing one is the defect.
    var byScenePhase: [ScanScenePhase: ScanThroughputBucket]

    init(
        foreground: ScanThroughputBucket = ScanThroughputBucket(),
        background: ScanThroughputBucket = ScanThroughputBucket(),
        unattributed: ScanThroughputBucket = ScanThroughputBucket(),
        byScenePhase: [ScanScenePhase: ScanThroughputBucket] = [:]
    ) {
        self.foreground = foreground
        self.background = background
        self.unattributed = unattributed
        self.byScenePhase = byScenePhase
    }

    subscript(bucket: ScanAttributionBucket) -> ScanThroughputBucket {
        switch bucket {
        case .foreground:   return foreground
        case .background:   return background
        case .unattributed: return unattributed
        }
    }

    var totalScanCount: Int {
        foreground.scanCount + background.scanCount + unattributed.scanCount
    }

    var attributedScanCount: Int {
        foreground.scanCount + background.scanCount
    }

    /// Share of eligible rows that could be attributed to a side at all.
    ///
    /// `nil` when there are no eligible rows — the same discipline as
    /// ``ScanThroughputBucket/realtimeRatio``: an absent denominator yields an
    /// absent answer, not `0` (which would read as "nothing is attributable")
    /// and not `1` (which would read as "everything is").
    ///
    /// Read this BEFORE reading the ratios. A split over a corpus that is 95 %
    /// unattributed is not a foreground-versus-background finding; it is a
    /// report that the corpus predates the instrumentation.
    var attributedFraction: Double? {
        guard totalScanCount > 0 else { return nil }
        return Double(attributedScanCount) / Double(totalScanCount)
    }

    /// **THE routing decision, isolated so it can be pinned by a test.**
    ///
    /// A `nil` scene phase is `.unattributed`. It is not `.foreground`, and the
    /// day someone "simplifies" this to `?? .active` the whole bead is undone
    /// silently — the numbers keep coming out, they are just wrong in a way
    /// nothing downstream can detect. `SemanticScanRunAttributionTests` bites
    /// exactly here.
    static func bucket(for scenePhase: ScanScenePhase?) -> ScanAttributionBucket {
        guard let scenePhase else { return .unattributed }
        return scenePhase.attributionBucket
    }

    /// True when this row can contribute a throughput measurement at all.
    ///
    /// The filters are not cosmetic:
    ///   * only `.success` rows have a latency that measures completed work;
    ///   * a nil `latencyMs` has no numerator;
    ///   * a non-positive window has no denominator;
    ///   * a `noWork:` sentinel spans a range it never examined (playhead-pz32),
    ///     so counting its ~zero latency against its whole-episode span would
    ///     report a model of spectacular speed that never ran.
    static func isEligible(_ row: SemanticScanResult) -> Bool {
        row.status == .success
            && row.latencyMs != nil
            && row.windowEndTime > row.windowStartTime
            && !row.isNoWorkSentinel
    }

    static func compute(rows: [SemanticScanResult]) -> SemanticScanThroughputSplit {
        var split = SemanticScanThroughputSplit()
        for row in rows where isEligible(row) {
            let audio = row.windowEndTime - row.windowStartTime
            let wall = (row.latencyMs ?? 0) / 1000.0
            switch bucket(for: row.scenePhase) {
            case .foreground:
                split.foreground.add(audioSeconds: audio, wallSeconds: wall)
            case .background:
                split.background.add(audioSeconds: audio, wallSeconds: wall)
            case .unattributed:
                split.unattributed.add(audioSeconds: audio, wallSeconds: wall)
            }
            if let phase = row.scenePhase {
                var bucket = split.byScenePhase[phase] ?? ScanThroughputBucket()
                bucket.add(audioSeconds: audio, wallSeconds: wall)
                split.byScenePhase[phase] = bucket
            }
        }
        return split
    }
}
