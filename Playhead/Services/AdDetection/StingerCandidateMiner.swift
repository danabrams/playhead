// StingerCandidateMiner.swift
// playhead-xsdz.42d: ON-DEVICE, gold-free per-show stinger CANDIDATE miner.
//
// WHAT THIS IS
// ------------
// A production Swift port of the VALIDATED xsdz.42d research prototype
// (`scripts/l2f-xsdz42d-multiplicity-stinger-mine.py`, verdict PARTIAL/GO as a
// candidate generator — see
// `playhead-baselines/xsdz42d-multiplicity-mining-spike-2026-07-16.md`). Given a
// show's episodes (their 50 Hz log-RMS envelopes) and the pipeline's own coarse
// ad-candidate regions per episode — NO gold labels anywhere — it emits a ranked
// list of stinger CANDIDATES per show. Each candidate is a 350-frame envelope
// template (the shipped `StingerBank` template shape) plus its offset relative to
// the candidate edge and a consensus/multiplicity score. The measured property is
// that the true stinger sits at rank <= 3 (top-1 on-boundary 6/9, in-region 9/9).
//
// WHAT THIS IS NOT — no live behavior
// -----------------------------------
// This is a candidate GENERATOR for a future per-user self-learning stinger bank.
// It is a PURE, deterministic, offline-invocable function with NO reference from
// `AdDetectionService.runBackfill`, `StingerRefiner`, or any live path; it reads
// no bundle, no filesystem, no network, and does not touch the shipped
// `StingerBank.json`. Wiring a confirmed candidate into the refiner (and snapping
// its edge with the rediff width oracle, xsdz.16) is a SEPARATE downstream bead.
// The miner cannot change playback, skip, or refinement behavior.
//
// ON-DEVICE MANDATE
// -----------------
// Envelope normalized cross-correlation + integer counting only — it REUSES the
// shipped `StingerRefiner.normalizedCrossCorrelation*` primitives (a direct port
// of the offline `ncc_curve`; the Python prototype's FFT `TargetScan` is
// documented bit-identical) and the shipped `StingerEnvelope`/`StingerBank`
// template shape. No ML training, no cloud, no gold. Deterministic: sorted
// episodes, fixed hop/radii/thresholds, no RNG, a total-order sort with an
// explicit insertion-index tiebreak, and order-preserving partitions — so fixed
// inputs always produce a fixed candidate ranking.
//
// ALGORITHM (three stages, faithful to xsdz.42/42b/42c/42d)
// ---------------------------------------------------------
//  1. SEEDED consensus (xsdz.42c): anchors = the start/end edge of every pipeline
//     ad-candidate span. For each source episode's anchor, sweep a 7 s template
//     window over ±`searchRadiusSeconds` at `deltaHopFrames`; a window earns a
//     cross-episode PARTNER when another episode has a same-type anchor whose
//     local envelope-NCC peak >= `consensusTau` at a break-relative offset within
//     `offsetToleranceSeconds` of the window's own offset. Keep windows with
//     vote (= 1 source + partners) >= `minEpisodeVote`; rank by
//     (vote, median partner NCC); non-max-suppress spatial + acoustic dupes to
//     `topK` distinct motifs.
//  2. WITHIN-EPISODE MULTIPLICITY (xsdz.42d): for each surviving motif and each
//     voting episode (source + partners), count DISTINCT same-type anchor firing
//     locations where the motif's local NCC peak >= `withinTau` (peaks NMS'd at
//     `nmsRadiusFrames` so adjacent anchors locking the same event count once). A
//     true mid-roll stinger fires near every break (mult 2-4); a once-per-episode
//     outro fires near one (mult 1).
//  3. DEMOTION re-rank (NOT a hard gate): a motif "passes" when >= `quorum` voting
//     episodes have multiplicity >= `multiplicityMin`. Ranking = stable partition
//     [passing] ++ [failing], each group in consensus order. On single-break
//     pre-roll shows nothing multiplies, so the order is identical to stage 1 —
//     pre-roll wins are preserved. `hardGate` (contrast variant) instead DROPS
//     failing motifs (empties single-break shows — see the spike sensitivity
//     sweep); it defaults OFF.

import Foundation

// MARK: - Side

/// Which break edge a candidate anchors on. Mirrors the shipped `StingerBank`
/// pre/post split: `.pre` = break START edge, `.post` = break END edge.
enum MinedStingerSide: String, Sendable, Equatable {
    case pre
    case post
}

// MARK: - Inputs

/// One pipeline ad-candidate span (the miner's gold-free "where are the ads"
/// seed — the app's own coarse detection, NOT a label). Seconds on the
/// episode timeline.
struct StingerMinerAdCandidate: Sendable, Equatable {
    let startSeconds: Double
    let endSeconds: Double

    init(startSeconds: Double, endSeconds: Double) {
        self.startSeconds = startSeconds
        self.endSeconds = endSeconds
    }
}

/// One episode's mining inputs: its full-episode 50 Hz log-RMS envelope (the
/// same transform `StingerEnvelope.compute` applies to shard PCM at runtime)
/// and the pipeline's coarse ad-candidate spans. `id` is the deterministic
/// sort key (episodes are mined in ascending `id` order, matching the
/// prototype's sorted-filename reference selection).
struct StingerMinerEpisode: Sendable {
    let id: String
    let envelope: [Float]
    let adCandidates: [StingerMinerAdCandidate]

    init(id: String, envelope: [Float], adCandidates: [StingerMinerAdCandidate]) {
        self.id = id
        self.envelope = envelope
        self.adCandidates = adCandidates
    }
}

// MARK: - Output

/// A ranked stinger candidate: an envelope template (shipped `StingerBank`
/// template shape — `templateFrames` of 50 Hz log-RMS) + edge offset + score.
/// This is NOT a bank entry; turning a confirmed candidate into a
/// `StingerTemplate` (and snapping its edge to the true boundary via the rediff
/// oracle) is a separate downstream step.
struct MinedStingerCandidate: Sendable, Equatable {
    /// One cross-episode partner: the episode it matched in and the peak NCC.
    struct Partner: Sendable, Equatable {
        let episodeId: String
        let peakNCC: Double
    }

    /// Break edge this candidate anchors on (`.pre` = start, `.post` = end).
    let side: MinedStingerSide
    /// The `templateFrames`-length 50 Hz log-RMS envelope motif — bank shape.
    let template: [Float]
    /// Episode the template was extracted from.
    let sourceEpisodeId: String
    /// Seconds of the pipeline candidate edge (rounded 0.1) the template is
    /// offset from. Gold-free — this is the coarse pipeline edge, not a true
    /// boundary.
    let anchorSeconds: Double
    /// Seconds of the template CENTER on the source episode timeline
    /// (rounded 0.1).
    let centerSeconds: Double
    /// Template center minus candidate edge (rounded 0.1). The candidate edge
    /// therefore lies `templateFrames / 2 / envelopeHz - edgeOffsetSeconds`
    /// seconds into the template; a downstream bead maps that (plus a rediff
    /// snap) to a `StingerTemplate.edgeSampleIndex`.
    let edgeOffsetSeconds: Double
    /// Consensus vote: 1 (source) + number of partner episodes.
    let vote: Int
    /// Median partner peak NCC (rounded 3) — the secondary consensus rank key.
    let medianPartnerNCC: Double
    /// The cross-episode partners backing the vote.
    let partners: [Partner]
    /// Within-episode multiplicity per voting episode (source + partners):
    /// distinct same-type anchor firing locations the motif recurs near.
    let withinMultiplicity: [String: Int]
    /// Number of voting episodes (source + deduped partners).
    let votingEpisodeCount: Int
    /// Whether the motif clears the multiplicity demotion under the config's
    /// `multiplicityMin` / `quorum` (passing candidates rank above failing).
    let multiplicityPasses: Bool
    /// Fraction of template frames below the silence threshold (rounded 2) —
    /// the silence-gate diagnostic; gated windows never become candidates.
    let silentFraction: Double
}

// MARK: - Config

/// Miner parameters. Defaults are the xsdz.42d PRIMARY config
/// (`WITHIN_TAU=0.75, MULT_MIN=2, quorum=any`, silence gate ON, demotion —
/// not hard-gate). Every field is parametric so the sensitivity sweep and the
/// parity harness can reproduce any prototype variant.
struct StingerMinerConfig: Sendable, Equatable {
    /// Envelope frame rate (must match the templates' rate).
    var envelopeHz: Int = 50
    /// Template length in frames (350 = 7 s = shipped bank template length).
    var templateFrames: Int = 350
    /// Search half-width around each candidate anchor edge (covers boundary
    /// undersizing up to ~40 s; playhead-4xqf).
    var searchRadiusSeconds: Double = 45.0
    /// Relative-offset sweep stride in frames (50 = 1.0 s).
    var deltaHopFrames: Int = 50
    /// Acoustic NCC partnership threshold.
    var consensusTau: Double = 0.75
    /// How consistent the break-relative offset must be across episodes (s).
    var offsetToleranceSeconds: Double = 10.0
    /// Minimum vote (source + partners) for a surviving consensus cluster.
    var minEpisodeVote: Int = 2
    /// Distinct consensus motifs reported per show.
    var topK: Int = 8
    /// Drop mostly-silent templates (splice-silence, not a musical stinger).
    var silenceGateEnabled: Bool = true
    /// log1p-RMS below this counts as effectively silent.
    var silentEnvelopeThreshold: Float = 0.5
    /// Drop templates whose silent fraction is >= this (silence gate).
    var silentFractionGate: Double = 0.30
    /// Acoustic-dupe NCC for consensus NMS (suppress near-identical motifs).
    var acousticDedupeNCC: Double = 0.85
    /// Spatial NMS radius in frames (distinct firing locations >= this apart).
    var nmsRadiusFrames: Int = 350
    /// Within-episode acoustic-match threshold for multiplicity.
    var withinTau: Double = 0.75
    /// Distinct within-episode firings that count as "multiplies".
    var multiplicityMin: Int = 2
    /// Quorum of voting episodes that must multiply.
    var quorum: Quorum = .any
    /// Hard-gate (drop failing motifs) instead of stable demotion. OFF by
    /// default — the sweep shows it empties single-break pre-roll shows.
    var hardGate: Bool = false

    enum Quorum: String, Sendable, Equatable {
        /// >= 1 voting episode multiplies.
        case any
        /// >= ceil(V/2) voting episodes multiply.
        case majority
    }

    static let `default` = StingerMinerConfig()
}

// MARK: - StingerCandidateMiner

enum StingerCandidateMiner {

    /// Mine ranked stinger candidates for one show from its episodes' envelopes
    /// and pipeline ad-candidate regions. Returns `[]` when fewer than two
    /// episodes carry candidate spans (cross-episode consensus is undefined).
    /// Pure and deterministic; heavy NCC work is intended to run off any hot
    /// actor (call from a detached background task).
    static func mine(
        episodes: [StingerMinerEpisode],
        config: StingerMinerConfig = .default
    ) -> [MinedStingerCandidate] {
        // The shipped NCC primitive bakes in `StingerEnvelope.envelopeHz` (50) as
        // its 1 s min-length gate; every second<->frame conversion below assumes
        // the same rate. A divergent `config.envelopeHz` would run to completion
        // and emit silently-wrong offsets/anchors, so reject it loudly instead.
        precondition(
            config.envelopeHz == StingerEnvelope.envelopeHz,
            "StingerCandidateMiner requires \(StingerEnvelope.envelopeHz) Hz envelopes "
            + "(config.envelopeHz was \(config.envelopeHz)) to match the shipped NCC primitive."
        )
        let hz = config.envelopeHz
        let templateLength = config.templateFrames
        let half = templateLength / 2
        let radiusFrames = Int((config.searchRadiusSeconds * Double(hz)).rounded(.toNearestOrEven))

        // Candidate-bearing episodes only, in deterministic id order.
        let candidateEpisodes = episodes
            .filter { !$0.adCandidates.isEmpty }
            .sorted { $0.id < $1.id }
        guard candidateEpisodes.count >= 2 else { return [] }

        let miningEpisodes = candidateEpisodes.map {
            buildEpisode($0, config: config, templateLength: templateLength,
                         half: half, radiusFrames: radiusFrames)
        }
        var episodesById: [String: MiningEpisode] = [:]
        for episode in miningEpisodes { episodesById[episode.id] = episode }

        // Stage 1 — consensus (ranked, NMS'd motifs in consensus order).
        let motifs = mineConsensus(miningEpisodes, config: config,
                                   templateLength: templateLength, half: half, hz: hz,
                                   radiusFrames: radiusFrames)

        // Stage 2 — within-episode multiplicity annotation (primary within_tau).
        let annotated = motifs.map {
            annotateMultiplicity($0, episodesById: episodesById, config: config,
                                 templateLength: templateLength, half: half, hz: hz)
        }

        // Build candidates in CONSENSUS order (each carries its multiplicity
        // pass/fail), then stage 3 — the stable demotion re-rank.
        let consensusOrder = annotated.map {
            buildCandidate($0, episodes: miningEpisodes,
                           templateLength: templateLength, config: config)
        }
        return stableMultiplicityRank(consensusOrder, hardGate: config.hardGate)
    }

    // MARK: - Stage 3 (pure, directly testable)

    /// Stable demotion re-rank: passing motifs first (in consensus order), then
    /// failing (in consensus order). `hardGate` DROPS failing motifs instead.
    /// Order-preserving `filter` == the prototype's stable list partition.
    static func stableMultiplicityRank(
        _ candidates: [MinedStingerCandidate],
        hardGate: Bool
    ) -> [MinedStingerCandidate] {
        let passing = candidates.filter { $0.multiplicityPasses }
        if hardGate { return passing }
        let failing = candidates.filter { !$0.multiplicityPasses }
        return passing + failing
    }

    /// Whether a motif clears the multiplicity demotion: `n_multi` voting
    /// episodes with within-multiplicity >= `multiplicityMin`, compared to the
    /// quorum (`any` = >= 1, `majority` = >= ceil(V/2)).
    static func passesMultiplicity(
        withinMultiplicity: [String: Int],
        votingEpisodeCount: Int,
        multiplicityMin: Int,
        quorum: StingerMinerConfig.Quorum
    ) -> Bool {
        let multiplyingEpisodes = withinMultiplicity.values.filter { $0 >= multiplicityMin }.count
        let need: Int
        switch quorum {
        case .majority: need = Int((Double(votingEpisodeCount) / 2.0).rounded(.up))
        case .any: need = 1
        }
        return multiplyingEpisodes >= need
    }

    // MARK: - Internal episode model

    private struct AnchorScan {
        let side: MinedStingerSide
        /// First frame of the local search sub-array in episode coordinates.
        let subStartFrame: Int
        /// The candidate edge frame this scan is centered on.
        let anchorFrame: Int
        /// The local envelope sub-array covering every window whose center is
        /// within ±radius of the anchor.
        let sub: [Float]
    }

    private struct MiningEpisode {
        let id: String
        let env: [Float]
        /// (anchorFrame, side) for every candidate span edge, in span order.
        let anchors: [(frame: Int, side: MinedStingerSide)]
        /// Precomputed local scans, one per anchor whose sub-array is >= one
        /// template long (shorter anchors cannot host a window — dropped, as in
        /// the prototype's `Episode.ascan`).
        let scans: [AnchorScan]
    }

    private struct ConsensusMotif {
        let sourceIndex: Int
        let sourceEpisodeId: String
        let side: MinedStingerSide
        let anchorFrame: Int
        let refStartFrame: Int
        let anchorSeconds: Double
        let centerSeconds: Double
        let edgeOffsetSeconds: Double
        let vote: Int
        let medianPartnerNCC: Double
        let partners: [MinedStingerCandidate.Partner]
        let silentFraction: Double
    }

    private struct AnnotatedMotif {
        let motif: ConsensusMotif
        let withinMultiplicity: [String: Int]
        let votingEpisodeCount: Int
    }

    private static func buildEpisode(
        _ input: StingerMinerEpisode,
        config: StingerMinerConfig,
        templateLength: Int,
        half: Int,
        radiusFrames: Int
    ) -> MiningEpisode {
        let hz = config.envelopeHz
        let env = input.envelope
        var anchors: [(frame: Int, side: MinedStingerSide)] = []
        anchors.reserveCapacity(input.adCandidates.count * 2)
        for span in input.adCandidates {
            anchors.append((Int((span.startSeconds * Double(hz)).rounded(.toNearestOrEven)), .pre))
            anchors.append((Int((span.endSeconds * Double(hz)).rounded(.toNearestOrEven)), .post))
        }
        var scans: [AnchorScan] = []
        scans.reserveCapacity(anchors.count)
        for anchor in anchors {
            let a0 = max(0, anchor.frame - radiusFrames - half)
            let b0 = min(env.count, anchor.frame + radiusFrames + half)
            guard b0 - a0 >= templateLength else { continue }
            scans.append(AnchorScan(
                side: anchor.side,
                subStartFrame: a0,
                anchorFrame: anchor.frame,
                sub: Array(env[a0..<b0])
            ))
        }
        return MiningEpisode(id: input.id, env: env, anchors: anchors, scans: scans)
    }

    // MARK: - Stage 1: consensus

    private static func mineConsensus(
        _ episodes: [MiningEpisode],
        config: StingerMinerConfig,
        templateLength: Int,
        half: Int,
        hz: Int,
        radiusFrames: Int
    ) -> [ConsensusMotif] {
        var protos: [ProtoMotif] = []
        var order = 0

        for (sourceIndex, source) in episodes.enumerated() {
            let others = episodes.enumerated().filter { $0.offset != sourceIndex }.map { $0.element }
            for anchor in source.anchors {
                let lo = anchor.frame - radiusFrames
                let hi = anchor.frame + radiusFrames
                var center = lo
                while center <= hi {
                    defer { center += config.deltaHopFrames }
                    let startFrame = center - half
                    guard startFrame >= 0, startFrame + templateLength <= source.env.count else { continue }
                    let template = Array(source.env[startFrame..<(startFrame + templateLength)])
                    let silentFraction = roundTo(
                        Double(template.filter { $0 < config.silentEnvelopeThreshold }.count) / Double(templateLength),
                        places: 2
                    )
                    if config.silenceGateEnabled && silentFraction >= config.silentFractionGate { continue }
                    let delta = Double(center - anchor.frame) / Double(hz)

                    var partners: [MinedStingerCandidate.Partner] = []
                    for other in others {
                        var best: Double?
                        for scan in other.scans where scan.side == anchor.side {
                            guard let peak = anchorPeak(
                                template: template, scan: scan, half: half, hz: hz
                            ) else { continue }
                            if peak.value >= config.consensusTau,
                               abs(peak.relativeOffsetSeconds - delta) <= config.offsetToleranceSeconds {
                                if best == nil || peak.value > best! { best = peak.value }
                            }
                        }
                        if let best {
                            partners.append(.init(episodeId: other.id, peakNCC: roundTo(best, places: 3)))
                        }
                    }
                    let vote = 1 + partners.count
                    guard vote >= config.minEpisodeVote else { continue }
                    let median = partners.isEmpty ? 0.0 : medianOf(partners.map(\.peakNCC))

                    protos.append(ProtoMotif(
                        order: order,
                        sourceIndex: sourceIndex,
                        sourceEpisodeId: source.id,
                        side: anchor.side,
                        anchorFrame: anchor.frame,
                        refStartFrame: startFrame,
                        anchorSeconds: roundTo(Double(anchor.frame) / Double(hz), places: 1),
                        centerSeconds: roundTo(Double(center) / Double(hz), places: 1),
                        edgeOffsetSeconds: roundTo(delta, places: 1),
                        vote: vote,
                        medianPartnerNCC: roundTo(median, places: 3),
                        partners: partners,
                        silentFraction: silentFraction
                    ))
                    order += 1
                }
            }
        }

        // Rank: vote desc, median desc, source id asc, center asc, insertion asc
        // (the trailing insertion index makes the order total — Swift's sort is
        // not guaranteed stable, so we cannot rely on Timsort stability).
        protos.sort { a, b in
            if a.vote != b.vote { return a.vote > b.vote }
            if a.medianPartnerNCC != b.medianPartnerNCC { return a.medianPartnerNCC > b.medianPartnerNCC }
            if a.sourceEpisodeId != b.sourceEpisodeId { return a.sourceEpisodeId < b.sourceEpisodeId }
            if a.centerSeconds != b.centerSeconds { return a.centerSeconds < b.centerSeconds }
            return a.order < b.order
        }

        // NMS to distinct motifs: suppress spatial dupes (same source + side
        // within nmsRadius) and acoustic dupes (NCC >= acousticDedupeNCC).
        var kept: [ConsensusMotif] = []
        var keptTemplates: [[Float]] = []
        for proto in protos {
            let template = Array(
                episodes[proto.sourceIndex].env[proto.refStartFrame..<(proto.refStartFrame + templateLength)]
            )
            var isDuplicate = false
            for (index, keptMotif) in kept.enumerated() {
                if keptMotif.sourceEpisodeId == proto.sourceEpisodeId,
                   keptMotif.side == proto.side,
                   abs(keptMotif.refStartFrame - proto.refStartFrame) < config.nmsRadiusFrames {
                    isDuplicate = true
                    break
                }
                let ncc = StingerRefiner.normalizedCrossCorrelationPeak(
                    template: keptTemplates[index], target: template
                )?.peak ?? 0.0
                if ncc >= config.acousticDedupeNCC {
                    isDuplicate = true
                    break
                }
            }
            if isDuplicate { continue }
            kept.append(ConsensusMotif(
                sourceIndex: proto.sourceIndex,
                sourceEpisodeId: proto.sourceEpisodeId,
                side: proto.side,
                anchorFrame: proto.anchorFrame,
                refStartFrame: proto.refStartFrame,
                anchorSeconds: proto.anchorSeconds,
                centerSeconds: proto.centerSeconds,
                edgeOffsetSeconds: proto.edgeOffsetSeconds,
                vote: proto.vote,
                medianPartnerNCC: proto.medianPartnerNCC,
                partners: proto.partners,
                silentFraction: proto.silentFraction
            ))
            keptTemplates.append(template)
            if kept.count >= config.topK { break }
        }
        return kept
    }

    private struct ProtoMotif {
        let order: Int
        let sourceIndex: Int
        let sourceEpisodeId: String
        let side: MinedStingerSide
        let anchorFrame: Int
        let refStartFrame: Int
        let anchorSeconds: Double
        let centerSeconds: Double
        let edgeOffsetSeconds: Double
        let vote: Int
        let medianPartnerNCC: Double
        let partners: [MinedStingerCandidate.Partner]
        let silentFraction: Double
    }

    // MARK: - Stage 2: within-episode multiplicity

    private static func annotateMultiplicity(
        _ motif: ConsensusMotif,
        episodesById: [String: MiningEpisode],
        config: StingerMinerConfig,
        templateLength: Int,
        half: Int,
        hz: Int
    ) -> AnnotatedMotif {
        let source = episodesById[motif.sourceEpisodeId]
        let template = source.map {
            Array($0.env[motif.refStartFrame..<(motif.refStartFrame + templateLength)])
        } ?? []

        // Voting episodes = source ++ partners, first-occurrence dedupe.
        var seen = Set<String>()
        var voting: [String] = []
        for id in [motif.sourceEpisodeId] + motif.partners.map(\.episodeId) where seen.insert(id).inserted {
            voting.append(id)
        }

        var multiplicity: [String: Int] = [:]
        for id in voting {
            if let episode = episodesById[id] {
                multiplicity[id] = withinEpisodeMultiplicity(
                    episode: episode, template: template, side: motif.side,
                    tau: config.withinTau, half: half, hz: hz,
                    nmsRadiusFrames: config.nmsRadiusFrames
                )
            } else {
                multiplicity[id] = 0
            }
        }
        return AnnotatedMotif(motif: motif, withinMultiplicity: multiplicity,
                              votingEpisodeCount: voting.count)
    }

    /// Distinct same-side anchor firing LOCATIONS in one episode where the
    /// motif's local NCC peak >= `tau`. Greedy peak-descending NMS at
    /// `nmsRadiusFrames` so two adjacent anchors locking the same acoustic
    /// event count once — the mid-roll-vs-outro discriminator.
    private static func withinEpisodeMultiplicity(
        episode: MiningEpisode,
        template: [Float],
        side: MinedStingerSide,
        tau: Double,
        half: Int,
        hz: Int,
        nmsRadiusFrames: Int
    ) -> Int {
        var hits: [(frame: Double, peak: Double, order: Int)] = []
        var order = 0
        for scan in episode.scans where scan.side == side {
            defer { order += 1 }
            guard let peak = anchorPeak(template: template, scan: scan, half: half, hz: hz) else { continue }
            if peak.value >= tau {
                // abs firing frame == the best window's center frame.
                let centerFrame = Double(scan.anchorFrame) + peak.relativeOffsetSeconds * Double(hz)
                hits.append((frame: centerFrame, peak: peak.value, order: order))
            }
        }
        // Strongest first; ties keep anchor-scan order so the greedy
        // independent-set count matches the prototype's stable sort (equal
        // peaks from identical planted motifs otherwise flip the NMS count).
        hits.sort { $0.peak != $1.peak ? $0.peak > $1.peak : $0.order < $1.order }
        var kept: [Double] = []
        for hit in hits where kept.allSatisfy({ abs(hit.frame - $0) >= Double(nmsRadiusFrames) }) {
            kept.append(hit.frame)
        }
        return kept.count
    }

    // MARK: - Shared NCC (reuses the shipped StingerRefiner primitive)

    /// Best NCC peak (and its rel-offset in s) of `template` over one anchor's
    /// local sub-array. Port of the prototype's `anchor_peak` — argmax of the
    /// NCC curve, first index of the maximum (numpy `argmax` semantics; the
    /// shipped `normalizedCrossCorrelationPeak` keeps the first max).
    private static func anchorPeak(
        template: [Float],
        scan: AnchorScan,
        half: Int,
        hz: Int
    ) -> (value: Double, relativeOffsetSeconds: Double)? {
        guard let result = StingerRefiner.normalizedCrossCorrelationPeak(
            template: template, target: scan.sub
        ) else { return nil }
        let centerFrame = scan.subStartFrame + result.offset + half
        let relativeOffset = Double(centerFrame - scan.anchorFrame) / Double(hz)
        return (result.peak, relativeOffset)
    }

    // MARK: - Candidate assembly

    private static func buildCandidate(
        _ annotated: AnnotatedMotif,
        episodes: [MiningEpisode],
        templateLength: Int,
        config: StingerMinerConfig
    ) -> MinedStingerCandidate {
        let motif = annotated.motif
        let template = Array(
            episodes[motif.sourceIndex].env[motif.refStartFrame..<(motif.refStartFrame + templateLength)]
        )
        let passes = passesMultiplicity(
            withinMultiplicity: annotated.withinMultiplicity,
            votingEpisodeCount: annotated.votingEpisodeCount,
            multiplicityMin: config.multiplicityMin,
            quorum: config.quorum
        )
        return MinedStingerCandidate(
            side: motif.side,
            template: template,
            sourceEpisodeId: motif.sourceEpisodeId,
            anchorSeconds: motif.anchorSeconds,
            centerSeconds: motif.centerSeconds,
            edgeOffsetSeconds: motif.edgeOffsetSeconds,
            vote: motif.vote,
            medianPartnerNCC: motif.medianPartnerNCC,
            partners: motif.partners,
            withinMultiplicity: annotated.withinMultiplicity,
            votingEpisodeCount: annotated.votingEpisodeCount,
            multiplicityPasses: passes,
            silentFraction: motif.silentFraction
        )
    }

    // MARK: - Numeric helpers (match the prototype's rounding / median)

    /// `statistics.median`: middle of the sorted values, mean of the two middle
    /// for an even count.
    private static func medianOf(_ values: [Double]) -> Double {
        guard !values.isEmpty else { return 0.0 }
        let sorted = values.sorted()
        let count = sorted.count
        if count % 2 == 1 { return sorted[count / 2] }
        return (sorted[count / 2 - 1] + sorted[count / 2]) / 2.0
    }

    /// Round-half-to-even to `places` decimals — the faithful port of Python's
    /// `round(x, n)` (banker's rounding). Kept off the ranking hot path's
    /// tie-sensitivity by design (the parity corpus separates rank keys well).
    private static func roundTo(_ value: Double, places: Int) -> Double {
        let scale = pow(10.0, Double(places))
        return (value * scale).rounded(.toNearestOrEven) / scale
    }
}
