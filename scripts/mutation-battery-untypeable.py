#!/usr/bin/env python3
"""playhead-x0lb — the UNTYPEABLE mutation battery (TY series).

WHY THIS VERDICT DID NOT EXIST
------------------------------
Every other battery in this repo proves the same thing: revert a behaviour and a
named test goes red. KILLED means a test noticed. That is the right instrument
for a behaviour, and it is the WRONG instrument for this bead's defect class.

Eighteen instances across six beads were all one shape — two quantities share a
unit and one is read as the other — and the reason review kept paying for them is
that no test can see them. The numbers are of the same type, in the same range,
often equal on every fixture a test author would write. playhead-fil5 R3 divided
the transcript union by the duration and called the result reach: NOUGHT of
twelve field assets cleared the gate, the feature was dead, and every test
passed, because every fixture was single-chunk — the one shape where density and
reach coincide.

So the kill this class needs is not "a test fails". It is **"the code does not
COMPILE"**, and that is a strictly stronger statement: a red test can be made
green by editing the test, while a type error has to be argued with. This battery
records it as its own verdict, `UNTYPEABLE`, because reporting a compile failure
as KILLED would understate it and reporting it in the Swift battery's vocabulary
is not possible at all — `scripts/mutation-battery.sh` reads its verdict out of
Swift Testing console glyphs, which a build that never produced a test binary
does not emit.

    scripts/mutation-battery-untypeable.py            # run them all
    scripts/mutation-battery-untypeable.py --list
    scripts/mutation-battery-untypeable.py --only TY01

WHAT A RAIL IS HERE
-------------------
Each mutation restores, VERBATIM, the expression that shipped before this bead —
a list-relative bound written straight into an episode cursor, a density read as
a reach, the 5 s bridge tolerance swapped for the 60 s re-scan threshold. The
expectation is that the build FAILS.

**The failure KIND is checked, not just the failure.** A compile error is easy to
cause by accident, and a mutant that fails to build for a syntax reason proves
nothing about the types. Every rail names a fragment its diagnostic must contain
— usually the two type names the substitution confuses — and a build that fails
for any other reason is reported `WRONG-ERROR`, not `UNTYPEABLE`. This is the
same lesson `scripts/gate_baseline.py` learned: identity includes the kind.

One rail (TY99) is a VACUITY CONTROL: a real one-token source edit that MUST
still compile. If it does not, the harness is reporting failure unconditionally
and every UNTYPEABLE above it is worthless. It is deliberately an edit to the
`UnsoundCursorPromotionSite` tag, which is an inventory LABEL rather than a
type — so the control also states this battery's honest limit: the tag records
which filed defect a laundering site belongs to, and nothing type-checks that it
records the right one.

**The control is not optional and `--only` cannot drop it** (R2 review). The
first version let `--only TY05` print `control not run` and exit 0 — a partial
run reporting success with nothing holding it up, which is precisely how the
previous bead's lexical canary passed for five consecutive rounds. Any selection
that omits the control now has it appended, and a run that somehow reaches the
summary without one exits non-zero rather than printing a word.

`--check-inventory` is a SECOND, buildless preflight and it is deliberately
modest about itself. `UnsoundCursorPromotionSite` is presented in the source as
an inventory of the laundering sites, but the Swift test can only read
`allCases` — a property of the enum, not of the code. This ties the two
together: every case written exactly once as a `site:` argument, the number of
`unsoundPlanListPromotion(` calls equal to the number of cases, and no bare
`EpisodeSeconds(` or `EpisodeSeconds.init(` construction in the runner (which is
how R2 probe PR5 wrote a sixth promotion with the enum untouched, and how R3
re-wrote it in the spelling this clause used to miss). It is a LEXICAL tripwire
on one file and can be out-spelled like any other; see limit L-F in
`CoverageQuantities.swift`.

R6 review added a SECOND clause to it, `check_region_fabrication`, and the
reason is the same shape one layer down. R5 typed the four interval carriers and
recorded in limit L-I that "there is now exactly ONE such door per region and it
sits at the genuine boundary" — a claim about CALL SITES that reads as a claim
about reachability. `init()` and `append(start:end:)` are INTERNAL, so any file
in the module can assemble any region out of any numbers in three lines: probe
PJ1 built a `TranscribedRegion` from `fetchFastTranscriptCoveredRanges` in
`AnalysisJobRunner` and it COMPILED, reproducing playhead-9y9e's SHIPPED defect
one layer below rails TY32/TY34, which exist to stop exactly that. No type
closes it (Swift's only friend mechanism is file scope, and the two file moves
that would work are measured and rejected in the function's own docstring), so
region fabrication is CONFINED to the two producing files lexically. Same
worth as L-F's: it cannot stop a fabrication, only stop a new one landing
unnoticed.

COST
----
Each rail is one incremental `xcodebuild build` (no tests, no simulator boot).
Mutating `AnalysisStore.swift` recompiles a large module, so the whole series is
minutes rather than seconds. Do not run it concurrently with a gate from the same
worktree — they share `.derivedData` and the CLAUDE.md 16 GB ceiling.
"""

import argparse
import hashlib
import os
import pathlib
import re
import subprocess
import sys
import time

ROOT = pathlib.Path(__file__).resolve().parents[1]

STORE = "Playhead/Persistence/AnalysisStore/AnalysisStore.swift"
RUNNER = "Playhead/Services/AdDetection/BackfillJobRunner.swift"
CLAIM = "Playhead/Services/AdDetection/SemanticScanClaim.swift"
ACTIVITY = "Playhead/Services/Activity/ActivitySnapshotProvider.swift"
QUANTITIES = "Playhead/Persistence/AnalysisStore/CoverageQuantities.swift"
READINESS = "Playhead/Views/Library/EpisodePreparationReadiness.swift"
RECONCILER = "Playhead/Services/PreAnalysis/AnalysisJobReconciler.swift"
ENGINE = "Playhead/Services/TranscriptEngine/TranscriptEngineService.swift"
JOBRUNNER = "Playhead/Services/AnalysisJobRunner/AnalysisJobRunner.swift"

# name, file, description, old, new, diagnostic fragments the build MUST print
MUTATIONS = [
    (
        "TY01", RUNNER,
        "playhead-5pyq / rate-limit defer: the coarse walk's PLAN-LIST bound "
        "written straight into the EPISODE cursor, exactly as it shipped",
        """                        lastProcessedUpperBoundSec: EpisodeSeconds.unsoundPlanListPromotion(
                            coverage.lastCoveredUpperBoundSec,
                            site: .rateLimitDefer
                        )""",
        """                        lastProcessedUpperBoundSec: coverage.lastCoveredUpperBoundSec""",
        ["PlanListSeconds", "EpisodeSeconds"],
    ),
    (
        "TY02", RUNNER,
        "the fullEpisodeScan completion cursor: a SEGMENT-list bound written as "
        "an episode cursor — the expression that wrote 53FC53E3's 2,525.82 on a "
        "2,528 s episode measured at adScanFraction 0.0142",
        """                        lastProcessedUpperBoundSec: EpisodeSeconds.unsoundPlanListPromotion(
                            jobInputs.segments.last.map { PlanListSeconds($0.endTime) },
                            site: .segmentListCompletion
                        )""",
        """                        lastProcessedUpperBoundSec: jobInputs.segments.last.map { PlanListSeconds($0.endTime) }""",
        ["PlanListSeconds", "EpisodeSeconds"],
    ),
    (
        "TY03", RUNNER,
        "playhead-26od's incremental checkpoint: the walk bound as an episode "
        "cursor, mid-pass",
        """            lastProcessedUpperBoundSec: EpisodeSeconds.unsoundPlanListPromotion(
                upperBound,
                site: .coarseCheckpoint
            )""",
        """            lastProcessedUpperBoundSec: upperBound""",
        ["PlanListSeconds", "EpisodeSeconds"],
    ),
    (
        "TY04", RUNNER,
        "playhead-41mu R2 verbatim: the cursor rule's 'where this run's plans "
        "began' term taken from the EPISODE cursor instead of the handed-over "
        "list — the two agree on attempt 1 and diverge on every resume",
        "            firstPlannedStart: coverage.firstPlannedSegmentStartSec,",
        "            firstPlannedStart: prior?.lastProcessedUpperBoundSec,",
        ["EpisodeSeconds", "PlanListSeconds"],
    ),
    (
        "TY05", STORE,
        "playhead-fil5 R3 verbatim: the transcript DENSITY published as the "
        "ad-scan REACH. Nought of twelve field assets cleared the gate and every "
        "test passed, because single-chunk fixtures make the two equal",
        "        return ReachRatio(examined: adScanCoveredSec, ofDeclaredDuration: episodeDurationSec)",
        "        return transcriptDensity",
        ["DensityRatio", "ReachRatio"],
    ),
    (
        "TY06", STORE,
        "instance 9: the reach numerator taken from a WATERMARK instead of an "
        "AREA — the shape that made an episode where detection did WORSE read as "
        "MORE complete (AD5F3A0A's fast watermark is 4,280.7 s against a fast "
        "area of 1,645.9 s, a 2.6x over-report; R2 review re-derived the "
        "watermark — 4,280.9 was the asset COLUMN, which does not stand in here)",
        "        return ReachRatio(examined: adScanCoveredSec, ofDeclaredDuration: episodeDurationSec)",
        "        return ReachRatio(examined: fastTranscriptCoverageEndSec, ofDeclaredDuration: episodeDurationSec)",
        # R1: the numerator type is now AdScanSeconds, so the diagnostic names it
        # rather than the shared area type it used to name.
        ["WatermarkSeconds", "AdScanSeconds"],
    ),
    (
        "TY07", STORE,
        "instance 19, pre-loaded: the coverage reader bridges at playhead-a1x0's "
        "60 s RE-SCAN threshold instead of its own 5 s measuring tolerance. Dan's "
        "note on a1x0 is 'Two names, or the next reviewer finds the seventeenth "
        "instance of the family here'",
        # R5 re-anchor: the three nested generic calls collapsed into
        # `AdScanSeconds(examined:within:bridging:)`, so the tolerance now
        # arrives at a `bridging:` label. The SUBSTITUTION is untouched — the
        # 60 s re-scan threshold in the 5 s measuring slot — which is what the
        # rail is about; only its spelling moved with the fix.
        "                    bridging: AnalysisCoverageMath.adScanBridgeableGapSec",
        "                    bridging: RescanThresholdSec.adScanRescanWorthyGapSec",
        ["RescanThresholdSec", "BridgeToleranceSec"],
    ),
    (
        "TY08", CLAIM,
        "the readiness floor measured against a DENSITY — the consumer half of "
        "instance 5, where a 0.98 floor calibrated for reach is applied to a "
        "quantity that reaches 0.9885 on an episode with no scan row at all",
        "        return adScanFraction < AnalysisJobRunner.semanticBackfillSufficientAdScanFraction",
        "        return adScanFraction < DensityRatio(0.98)",
        ["ReachRatio", "DensityRatio"],
    ),
    (
        "TY09", STORE,
        "R1 review, planted and SURVIVED before the fix: playhead-fil5 R3 in its "
        "OTHER spelling — the transcript AREA divided by the duration inline, "
        "without going through `transcriptDensity`. Three areas shared one type, "
        "so the expression that killed the feature still compiled",
        "        return ReachRatio(examined: adScanCoveredSec, ofDeclaredDuration: episodeDurationSec)",
        "        return ReachRatio(examined: fastTranscriptCoveredSec, ofDeclaredDuration: episodeDurationSec)",
        ["CoveredSeconds", "AdScanSeconds"],
    ),
    (
        "TY10", STORE,
        "R1 review, planted and SURVIVED before the fix: the ANALYZED area as "
        "the reach numerator. `analysisCoveredSec` is the transcript union "
        "clipped to the DSP frontier, so it is large on an episode no semantic "
        "scan ever read",
        "        return ReachRatio(examined: adScanCoveredSec, ofDeclaredDuration: episodeDurationSec)",
        "        return ReachRatio(examined: analysisCoveredSec, ofDeclaredDuration: episodeDurationSec)",
        ["AnalyzedSeconds", "AdScanSeconds"],
    ),
    (
        "TY11", STORE,
        "R1 review, planted and SURVIVED before the fix: instance 9's own shape "
        "— `max(endTime)` of DETECTED ads substituted for the TRANSCRIPT's reach "
        "in the denominator-contradiction guard, so an episode where detection "
        "did worse reads as more complete",
        "        let transcriptReach: WatermarkSeconds? = [fastTranscriptCoverageEndSec, finalReach]",
        "        let transcriptReach: WatermarkSeconds? = [confirmedAdCoverageEndSec, finalReach]",
        ["FrontierSeconds", "WatermarkSeconds"],
    ),
    (
        "TY12", RUNNER,
        "R1 review: the cursor CASHED as a bare `Double`. `narrowedForResume` "
        "deletes every segment ending at or below its argument, permanently, so "
        "this is the single most consequential crossing in the bead",
        "        let inputs = Self.narrowedForResume(rootInputs, cursor: job.progressCursor?.lastProcessedUpperBoundSec)",
        "        let inputs = Self.narrowedForResume(rootInputs, cursor: job.progressCursor?.lastProcessedUpperBoundSec?.rawValue)",
        ["Double", "EpisodeSeconds"],
    ),
    (
        "TY13", RUNNER,
        "R1 review: the promotion rule's PRIOR-CURSOR slot fed this run's own "
        "plan-list bound — the other half of the 41mu R2 family, and the one "
        "TY04 does not cover",
        "            priorEpisodeCursor: prior?.lastProcessedUpperBoundSec,",
        "            priorEpisodeCursor: coverage.lastCoveredUpperBoundSec,",
        ["PlanListSeconds", "EpisodeSeconds"],
    ),
    (
        "TY14", RUNNER,
        "R1 review: the CURSOR RULE bridged at playhead-a1x0's 60 s re-scan "
        "threshold instead of the 5 s measuring tolerance. TY07 is the same "
        "confusion in the coverage numerator; this is it in the rule that "
        "decides what a resume may skip",
        "            bridge: AnalysisCoverageMath.adScanBridgeableGapSec",
        "            bridge: RescanThresholdSec.adScanRescanWorthyGapSec",
        ["RescanThresholdSec", "BridgeToleranceSec"],
    ),
    (
        "TY15", ACTIVITY,
        "R2 review, planted and SURVIVED before the fix: the AD-SCAN area as "
        "the Activity AN bar's numerator. The audit CLAIMED the typed numerator "
        "stopped this; the call site read `?.rawValue` off the summary, so every "
        "area on it fitted the `Double?` parameter",
        """            let analysisFraction = fraction(
                area: summary?.analysisCoveredSec,
                ofDeclaredDuration: summary?.episodeDurationSec
            )
            // Download fraction comes from the (already-snapshotted)""",
        """            let analysisFraction = fraction(
                area: summary?.adScanCoveredSec,
                ofDeclaredDuration: summary?.episodeDurationSec
            )
            // Download fraction comes from the (already-snapshotted)""",
        ["AdScanSeconds", "AnalyzedSeconds"],
    ),
    (
        "TY16", ACTIVITY,
        "R2 review: the same substitution at the DOGFOOD WIRE copy of the AN "
        "fraction. Two call sites, two helpers, one defect — a rail per site "
        "because fixing one and not the other is how this family survives",
        """            let analysisFraction = fraction(
                area: summary?.analysisCoveredSec,
                ofDeclaredDuration: summary?.episodeDurationSec
            )
            let fastTranscriptWatermarkSec""",
        """            let analysisFraction = fraction(
                area: summary?.fastTranscriptCoveredSec,
                ofDeclaredDuration: summary?.episodeDurationSec
            )
            let fastTranscriptWatermarkSec""",
        ["CoveredSeconds", "AnalyzedSeconds"],
    ),
    (
        "TY17", ACTIVITY,
        "R2 review, planted and SURVIVED before the fix: the ad-scan REACH "
        "rendered as the transcript-DENSITY bar. Instance 5's own shape at the "
        "surface the user reads, and it was one `?.rawValue` away from being "
        "unwritable",
        "            let transcriptFraction = transcriptBarFill(summary?.transcriptDensity)\n"
        "            let featureCoverageEndSec",
        "            let transcriptFraction = transcriptBarFill(summary?.adScanFraction)\n"
        "            let featureCoverageEndSec",
        ["ReachRatio", "DensityRatio"],
    ),
    (
        "TY18", ACTIVITY,
        "R3 review, planted and SURVIVED before the fix: the AN bar's "
        "DENOMINATOR taken from the transcript WATERMARK. R2 typed this "
        "helper's numerator and left `durationSec: Double`, which made it the "
        "one ratio in the path with a half-typed pair — ReachRatio and "
        "DensityRatio both name BOTH terms at their only constructor",
        """            let analysisFraction = fraction(
                area: summary?.analysisCoveredSec,
                ofDeclaredDuration: summary?.episodeDurationSec
            )
            // Download fraction comes from the (already-snapshotted)""",
        """            let analysisFraction = fraction(
                area: summary?.analysisCoveredSec,
                ofDeclaredDuration: summary?.fastTranscriptCoverageEndSec
            )
            // Download fraction comes from the (already-snapshotted)""",
        ["WatermarkSeconds", "EpisodeSeconds"],
    ),
    (
        "TY19", ACTIVITY,
        "R3 review: the same denominator substitution at the DOGFOOD WIRE copy "
        "— probe PC1's exact site. One rail per call site, for the reason R2 "
        "gave when it split TY15 from TY16: fixing one and not the other is how "
        "this family survives",
        """            let analysisFraction = fraction(
                area: summary?.analysisCoveredSec,
                ofDeclaredDuration: summary?.episodeDurationSec
            )
            let fastTranscriptWatermarkSec""",
        """            let analysisFraction = fraction(
                area: summary?.analysisCoveredSec,
                ofDeclaredDuration: summary?.fastTranscriptCoverageEndSec
            )
            let fastTranscriptWatermarkSec""",
        ["WatermarkSeconds", "EpisodeSeconds"],
    ),
    (
        "TY20", STORE,
        "R3 review, planted and SURVIVED before the fix: the ANALYZED area "
        "clipped to the TRANSCRIPT's own watermark instead of the DSP "
        "frontier. `unionedSecondsClipped`'s `upperBound` was a bare Double, "
        "and TY09/TY10 pin which INTERVALS go in, not which BOUND clips them — "
        "so AN would equal TX on every episode and the two bars would simply "
        "agree, which is the one shape of this family a reader cannot spot",
        "            if let frontier = analysisFrontierSec, fastCoveredSec != nil {",
        "            if let frontier = fastEndSec, fastCoveredSec != nil {",
        ["WatermarkSeconds", "FrontierSeconds"],
    ),
    (
        "TY21", READINESS,
        "R3 review, planted and SURVIVED before the fix: the analyze zone "
        "driven by the DOWNLOAD fraction, whose numerator is BYTES. The third "
        "instance of the spelling R2 found twice in ActivitySnapshotProvider — "
        "`?.rawValue` at the CALL SITE, so the type had stopped applying before "
        "the argument was read",
        "    let analysis = analyzeZoneFill(inputs.adScanFraction)",
        "    let analysis = analyzeZoneFill(inputs.downloadFraction)",
        ["Double", "ReachRatio"],
    ),
    (
        "TY22", STORE,
        "R4 review, planted and SURVIVED before the fix: the area-vs-duration "
        "sanity guard driven from the TRANSCRIPT area. Written in raw values "
        "the comparison accepted every area on the summary, so the check meant "
        "to catch a numerator that describes different audio could be pointed "
        "away from the numerator entirely (probe PA2)",
        "        guard !adScanCoveredSec.exceeds(episodeDurationSec, byMoreThan: tolerance) else { return nil }",
        "        guard !(fastTranscriptCoveredSec?.exceeds(episodeDurationSec, byMoreThan: tolerance) ?? false) else { return nil }",
        ["CoveredSeconds", "exceeds"],
    ),
    (
        "TY23", STORE,
        "R4 review, planted and SURVIVED before the fix: the reach-past-duration "
        "guard fed an AREA. The comment directly above it SAYS an area can never "
        "disprove a duration and only a reach past the declared end can — and "
        "probe PA3 wrote the area into the guard anyway and it compiled. A "
        "sentence forbidding a substitution beside an expression that permits it "
        "is instance 18's own shape",
        "        if let transcriptReach, transcriptReach.reaches(past: episodeDurationSec, byMoreThan: tolerance) {",
        "        if transcriptReach != nil, adScanCoveredSec.reaches(past: episodeDurationSec, byMoreThan: tolerance) {",
        ["AdScanSeconds", "reaches"],
    ),
    (
        "TY24", ACTIVITY,
        "R4 review, planted and SURVIVED before the fix: the Activity AN bar's "
        "ratio INVERTED. R3 typed both terms, which stops the wrong quantity "
        "arriving and does nothing once it has — inside the helper both were "
        "`Double` again and probe PB1 divided duration by area. It renders in "
        "the same [0,1] bar and clamps to 1.0 whenever the analyzed area is "
        "under the duration: a FULL bar on an episode nothing analyzed",
        """        func fraction(area: AnalyzedSeconds?, ofDeclaredDuration duration: EpisodeSeconds?) -> Double? {
            area?.fractionOfDeclaredDuration(duration)
        }""",
        """        func fraction(area: AnalyzedSeconds?, ofDeclaredDuration duration: EpisodeSeconds?) -> Double? {
            duration?.fractionOfDeclaredDuration(area)
        }""",
        ["EpisodeSeconds", "fractionOfDeclaredDuration"],
    ),
    (
        "TY25", ACTIVITY,
        "R4 review: the same inversion at the DOGFOOD WIRE copy (probe PB2), "
        "which compiled independently of PB1. One rail per call site for the "
        "reason R2 gave when it split TY15 from TY16",
        """    private func fraction(area: AnalyzedSeconds?, ofDeclaredDuration duration: EpisodeSeconds?) -> Double? {
        area?.fractionOfDeclaredDuration(duration)
    }""",
        """    private func fraction(area: AnalyzedSeconds?, ofDeclaredDuration duration: EpisodeSeconds?) -> Double? {
        duration?.fractionOfDeclaredDuration(area)
    }""",
        ["EpisodeSeconds", "fractionOfDeclaredDuration"],
    ),
    (
        "TY26", STORE,
        "R5 probe PA5, planted and COMPILED before the fix: the transcript "
        "REGION built from the DSP frontier. The frontier reaches the end of an "
        "episode nothing ever transcribed, and this one edit poisons the AN clip "
        "and the ad-scan bound at once — the interval arrays were all "
        "`[(start: Double, end: Double)]`, so the watermark stand-in accepted "
        "any position on the summary",
        "            } else if let transcriptCovered = fastCoveredSec {",
        "            } else if let transcriptCovered = analysisFrontierSec {",
        ["FrontierSeconds", "CoveredSeconds"],
    ),
    (
        "TY27", STORE,
        "R5 probe PA7, planted and COMPILED before the fix: the ad-scan area "
        "measured from `fastIntervals` instead of the scan windows. This is "
        "playhead-fil5 R3's own P0 — the transcript published as ad-scan reach — "
        "reproduced ONE LAYER BELOW every rail that looks for it, with "
        "`AdScanSeconds` intact on the box and TY05/TY06/TY09/TY10 all still "
        "green. It is the probe that decided this scope expansion",
        "                    examined: adScanIntervals[id] ?? ScannedRegion(),",
        "                    examined: fastIntervals[id] ?? ScannedRegion(),",
        ["FastTranscriptRegion", "ScannedRegion"],
    ),
    (
        "TY28", STORE,
        "R5 probe PA8, planted and COMPILED before the fix: the ad-scan bound "
        "narrowed from the readable region to the FAST pass alone — "
        "playhead-9y9e's defect verbatim, worth 55.4 % vs 100.0 % on 0C2FC22E "
        "per the comment at the site, and a ceiling below the 0.98 completion "
        "floor on the nine assets whose FAST-ONLY ceiling sits below it "
        "(0C2FC22E 2C5C3699 44F076BB 48E903D7 53FC53E3 58882C47 83592353 "
        "AD5F3A0A D9B513CD, re-derived at R6). The pull carries THREE "
        "different nines-of-twelve and R6 named the other two at their sites; "
        "this was the one it did not touch",
        "                    within: transcribedRegion,",
        "                    within: transcriptRegion,",
        ["FastTranscriptRegion", "TranscribedRegion"],
    ),
    (
        "TY29", STORE,
        "R5 probe PA9, planted and COMPILED before the fix: the AN clip's "
        "INTERVALS widened to both passes. TY20 pins which BOUND clips and R3 "
        "said so explicitly; it does not pin what is CLIPPED, and playhead-9y9e "
        "deliberately did not widen this quantity when it widened the ad-scan "
        "bound one block below",
        "                    clipping: transcriptRegion,",
        "                    clipping: transcribedRegion,",
        ["TranscribedRegion", "FastTranscriptRegion"],
    ),
    (
        "TY30", STORE,
        "R5 probe PF2, planted and COMPILED before the fix: the FAST query's "
        "rows accumulated into the FINAL region. The producer side had three "
        "dictionaries of three region types live in one scope and every region "
        "answers `append(start:end:)`, so typing the CONSUMERS (TY26-TY29) left "
        "the populations swappable where they are built",
        "            try readFastTranscriptRegions(ids: slice, into: &fastIntervals, maxEnd: &fastMaxEnd)",
        "            try readFastTranscriptRegions(ids: slice, into: &finalIntervals, maxEnd: &fastMaxEnd)",
        ["FinalTranscriptRegion", "FastTranscriptRegion"],
    ),
    (
        "TY31", STORE,
        "R5 probe PF4, planted and COMPILED before the fix: the SCAN WINDOWS "
        "poured into the FAST TRANSCRIPT region — PA7's confusion (scan read as "
        "transcript) moved to the PRODUCER, which is the same 'one layer below "
        "the rail' shape PA7 itself was. It corrupts TX and AN as well as the "
        "ad-scan area, so it is strictly wider than PA7",
        "            try readScannedRegions(ids: slice, into: &adScanIntervals, rowSeen: &adScanRowSeen)",
        "            try readScannedRegions(ids: slice, into: &fastIntervals, rowSeen: &adScanRowSeen)",
        ["FastTranscriptRegion", "ScannedRegion"],
    ),
    (
        "TY32", RECONCILER,
        "R5 review probe PG2, planted and COMPILED before the fix: the sweep's "
        "transcript floor measured over the FAST pass alone. This is "
        "playhead-9y9e's SHIPPED defect verbatim — 48E903D7 read 36.9 % against "
        "a 0.95 floor while its two passes cover 95.1 % — and it was writable "
        "again because `fetchTranscriptCoveredRanges` and "
        "`fetchFastTranscriptCoveredRanges` returned the identical bare "
        "`[(start: Double, end: Double)]`. A FIFTH producer, outside the four "
        "R5 enumerated inside `fetchCoverageSummariesByAssetIds`",
        "                    region: try await store.fetchTranscribedRegion(assetId: assetId)",
        "                    region: try await store.fetchFastTranscriptCoveredRanges(assetId: assetId)",
        ["TranscribedRegion", "(start: Double, end: Double)"],
    ),
    (
        "TY33", ENGINE,
        "playhead-6r4z INVERTED THIS RAIL, and the inversion is the finding. R5 "
        "wrote it as 'the both-pass region poured into the FAST-only shard-skip "
        "index, which would let final-pass coverage authorise SKIPPING fast-pass "
        "work' — and that index never skipped anything, it only re-orders "
        "(playhead-mptr's header argues the point at length, because the "
        "skipping shape was tried and two tests proved it a capability loss). So "
        "the direction R5 pinned was the defect: reading `pass = 'fast'` alone "
        "made audio the FINAL pass covers sort UNREAD and float to the FRONT of "
        "the pass minted to read the audio behind it — 215 shards / 6,450 s "
        "across seven of the twelve assets on the 2026-08-03 pull, and on "
        "48E903D7 a 1,230 s re-read prefix against 103 s of new audio inside a "
        "flat 300 s cap. The mutation is now the SHIPPED defect restored: the "
        "narrow population back at the widened call site",
        "                transcribedRegion: try await store.fetchTranscribedRegion(assetId: analysisAssetId)",
        "                transcribedRegion: try await store.fetchFastTranscriptCoveredRanges(assetId: analysisAssetId)",
        ["TranscribedRegion", "(start: Double, end: Double)"],
    ),
    (
        "TY34", JOBRUNNER,
        "R5 review probe PG8, planted and COMPILED before the fix: TY32's "
        "substitution at the runner's own copy of the same gate. Fixing one call "
        "site and not the other is how this family has survived four rounds",
        "        guard let region = try? await store.fetchTranscribedRegion(assetId: assetId),",
        "        guard let region = try? await store.fetchFastTranscriptCoveredRanges(assetId: assetId),",
        ["TranscribedRegion", "(start: Double, end: Double)"],
    ),
    (
        "TY35", JOBRUNNER,
        "R6 review probe PJ3, planted and COMPILED before the fix: the RAW "
        "interval union handed to the finalize floor, written in ONE TOKEN off "
        "the very region the correct expression takes. This is playhead-fil5 "
        "R3's P0 — and it does not loosen the gate, it turns it off: the raw "
        "union clears 0.95 for ZERO of the twelve assets on the 2026-08-03 pull",
        "            coveredSec: SemanticScanClaim.bridgedTranscriptCoveredSec(region: region),",
        "            coveredSec: region.unionedSeconds,",
        ["expected argument type 'BridgedTranscriptSeconds'"],
    ),
    (
        "TY36", JOBRUNNER,
        "R6 review probe PJ4: the fast WATERMARK as the finalize floor's AREA, "
        "in scope three lines above a doc paragraph saying in as many words that "
        "the watermark cannot be the gate. Latent instance L1 (playhead-fpnt), "
        "which the audit cleared with an argument rather than a probe — on the "
        "same pull D9B513CD reads 100.0 % by watermark against an 88.3 % area, "
        "and the 11.7 pp is across this floor",
        "            coveredSec: SemanticScanClaim.bridgedTranscriptCoveredSec(region: region),",
        "            coveredSec: watermark,",
        ["expected argument type 'BridgedTranscriptSeconds'"],
    ),
    (
        "TY37", RECONCILER,
        "R6 review probe PJ5: the finalize floor's NUMERATOR and DENOMINATOR "
        "exchanged. R4's PB1/PB2 reciprocal shape, closed for the Activity bars "
        "by moving the division into `fractionOfDeclaredDuration` and still "
        "writable at this gate because both slots were `Double?`",
        """                coveredSec: SemanticScanClaim.bridgedTranscriptCoveredSec(
                    region: try await store.fetchTranscribedRegion(assetId: assetId)
                ),
                episodeDurationSec: asset.episodeDurationSec.map { EpisodeSeconds($0) }""",
        """                coveredSec: asset.episodeDurationSec.map { EpisodeSeconds($0) },
                episodeDurationSec: SemanticScanClaim.bridgedTranscriptCoveredSec(
                    region: try await store.fetchTranscribedRegion(assetId: assetId)
                )""",
        ["BridgedTranscriptSeconds", "EpisodeSeconds"],
    ),
    (
        "TY38", JOBRUNNER,
        "R7 probe PK1, planted and COMPILED before the fix, WITH the R6 lexical "
        "rail returning rc=0: the fast WATERMARK modelled as a contiguous "
        "TRANSCRIBED REGION and handed to the 0.95 finalize floor, three lines "
        "under a doc paragraph saying the watermark cannot be the gate "
        "(D9B513CD reads 100.0 % by watermark against an 88.3 % two-pass area, "
        "which is across that floor). It evaded `check_region_fabrication` "
        "because dot-`.init` names no type, so no grep over TYPE NAMES can see "
        "it. What refuses it now is the compiler, via `RegionFillDoor`",
        """        guard let region = try? await store.fetchTranscribedRegion(assetId: assetId),
              !region.isEmpty else {
            return nil
        }""",
        """        let region: TranscribedRegion = .init(
            fastPass: .init(spanningFromZeroTo: CoveredSeconds(watermark)),
            finalPass: .init()
        )""",
        ["missing argument for parameter 'door'"],
    ),
    (
        "TY99", RUNNER,
        "VACUITY CONTROL: the laundering site's inventory TAG changed to a "
        "different filed defect. This MUST still compile — the tag records which "
        "bead owns a site and nothing type-checks that it records the right one. "
        "If this is reported UNTYPEABLE the harness fails unconditionally and "
        "every verdict above it is void",
        "                            site: .segmentListCompletion",
        "                            site: .rateLimitDefer",
        [],
    ),
]

EXPECT_COMPILES = {"TY99"}

SCHEME = "Playhead"
DESTINATION = "platform=iOS Simulator,name=iPhone 17"
DERIVED = ".derivedData"


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run_build():
    """Return (rc, combined output) of a build-only xcodebuild.

    `build`, never `test`: this battery's whole question is whether the SOURCE
    type-checks, so booting a simulator and running ~9,900 tests would cost
    minutes per rail and answer a different question.
    """
    env = dict(os.environ)
    env.setdefault("DEVELOPER_DIR", "/Applications/Xcode-beta.app/Contents/Developer")
    proc = subprocess.run(
        [
            "xcodebuild", "build",
            "-scheme", SCHEME,
            "-destination", DESTINATION,
            "-derivedDataPath", DERIVED,
            "-jobs", os.environ.get("PLAYHEAD_BUILD_JOBS", "4"),
        ],
        cwd=str(ROOT), capture_output=True, text=True, env=env,
    )
    return proc.returncode, proc.stdout + proc.stderr


def diagnostics(output):
    return [line.strip() for line in output.splitlines() if "error:" in line]


def check_inventory():
    """Tie ``UnsoundCursorPromotionSite`` to the SITES. Returns a list of faults.

    The Swift test can only read `allCases`, which is a property of the enum and
    proves nothing about the source; this is what makes the inventory a claim
    about the code. It is LEXICAL and therefore out-spellable — that is stated,
    not hidden, and limit L-F in `CoverageQuantities.swift` says so too.

    The third clause is the one with teeth. R2 probe PR5 wrote a sixth unsound
    promotion as `bound.map { EpisodeSeconds($0.rawValue) }`: it compiled, the
    enum was untouched and the Swift test stayed green, so a bare
    `EpisodeSeconds(` construction anywhere in the runner is treated as an
    unlogged promotion until proven otherwise.

    R3 review re-planted that probe in the `.init` spelling and this preflight
    RETURNED 0 — a lexical rail out-spelled inside its own file, which is the
    failure mode the whole bead is a reaction to. Both spellings are counted
    now. That is a patch on one hole, not a proof: the clause remains lexical
    and a helper declared in another file still walks past it (L-F), which is
    why the sentence above says tripwire and not proof.
    """
    faults = []
    quantities = (ROOT / QUANTITIES).read_text(encoding="utf-8")
    runner = (ROOT / RUNNER).read_text(encoding="utf-8")

    block = quantities.split("enum UnsoundCursorPromotionSite", 1)
    if len(block) != 2:
        return ["UnsoundCursorPromotionSite not found in %s" % QUANTITIES]
    body = block[1].split("\n}", 1)[0]
    cases = [line.split("case ", 1)[1].strip()
             for line in body.splitlines() if line.strip().startswith("case ")]
    if not cases:
        return ["UnsoundCursorPromotionSite has no cases — parse failed"]

    for case in cases:
        used = runner.count("site: .%s" % case)
        if used != 1:
            faults.append("case .%s is written at %d site(s), expected exactly 1"
                          % (case, used))

    calls = runner.count("EpisodeSeconds.unsoundPlanListPromotion(")
    if calls != len(cases):
        faults.append("%d unsoundPlanListPromotion( call(s) against %d case(s) — "
                      "the inventory and the sites have drifted apart"
                      % (calls, len(cases)))

    # `EpisodeSeconds.promoting(` / `.unsoundPlanListPromotion(` (the two named
    # promotions) are fine; a raw construction is the bypass L-F describes.
    #
    # R3 review: `EpisodeSeconds.init(` is counted TOO, and it was not. A sixth
    # promotion written `bound.map { EpisodeSeconds.init($0.rawValue) }` — five
    # tagged sites left intact — passed this preflight with rc=0, which is a
    # tripwire failing to fire in its own stated scope rather than the
    # out-of-file case L-F already concedes. SwiftLint's `explicit_init` does
    # reject that spelling, so the tree was never actually open; but a rail
    # whose only backstop is a different tool is a rail nobody can reason
    # about, and this file is the one that claims the inventory is tied to the
    # sites.
    spellings = ["EpisodeSeconds(", "EpisodeSeconds.init("]
    raw = sum(runner.count(s) for s in spellings)
    if raw:
        faults.append("%d bare `EpisodeSeconds(`/`EpisodeSeconds.init(` "
                      "construction(s) in %s — a PlanList→Episode promotion "
                      "that no site tag records" % (raw, RUNNER))
    faults.extend(check_region_fabrication())
    return faults


# The four interval carriers, and the only two production files allowed to
# FABRICATE one. See `check_region_fabrication`.
REGION_TYPES = (
    "FastTranscriptRegion",
    "FinalTranscriptRegion",
    "TranscribedRegion",
    "ScannedRegion",
)
REGION_PRODUCERS = {STORE, QUANTITIES}
# The token every region assembly must NAME, and how many entry points demand
# it (`spanningFromZeroTo`, `fastPass/finalPass`, and four `append`s).
REGION_FILL_DOOR_TOKEN = "openedByCoverageReader"
REGION_FILL_DOOR_COUNT = 6


def check_region_fabrication():
    """Confine region ASSEMBLY to the two files that legitimately produce one.

    **R7 rewrote this, because R6's version had three holes and one of them
    compiled.** R6 confined fabrication by grepping for the four region TYPE
    NAMES followed by `(`, plus `.append(start:`. R7 planted against that grep:

      * `TranscribedRegion ()` — one space before the parens. Swift accepts it;
        the pattern required `\(` immediately.
      * `.append(start : 0, end: 1)` — one space before the label's colon. Same.
      * **probe PK1**, the one that mattered:

            let region: TranscribedRegion = .init(
                fastPass: .init(spanningFromZeroTo: CoveredSeconds(watermark)),
                finalPass: .init()
            )

        planted at `AnalysisJobRunner.transcriptCoverageOfCompletedTranscript`,
        where it models the fast WATERMARK as a contiguous transcribed region and
        feeds it to the 0.95 finalize floor — three lines under a doc paragraph
        saying the watermark cannot be the gate (D9B513CD reads 100.0 % by
        watermark against an 88.3 % two-pass area, across that very floor). This
        preflight returned **rc=0** and the app **BUILT**.

    Dot-`.init` names no type, so NO pattern over type names can ever see it.
    That is not a hole to patch, it is the shape of the instrument.

    **What replaced it: a compile-enforced naming obligation.** ``RegionFillDoor``
    is a token the six assembly entry points now take, so any expression that
    builds a non-empty region must write ``openedByCoverageReader``. A static
    member has no spelling that omits its own name, so the grep below went from
    "the spellings R6 thought of" to "one identifier, every spelling" — and the
    type checker, not this script, is what refuses an omission
    (`missing argument for parameter 'door'`, rail TY38).

    **Say plainly what it is NOT.** The token is `internal`, so any file in the
    module can write it; this stops an author who is not thinking about which
    population they hold, not one who is determined. It is L-F's worth, arrived
    at honestly rather than by enumerating spellings. The one bypass left is
    laundering a token through a producer file — visible, weird, and greppable.

    The type-name clause is KEPT and hardened (whitespace-tolerant) because the
    EMPTY `init()` deliberately takes no door: an empty region is an absence, and
    it errs toward under-claiming. Scoped to production (`Playhead/`); tests
    fabricate on purpose, which is what fixtures are for.
    """
    faults = []
    names = "|".join(REGION_TYPES)
    # `Type (`, `.append( start :` — both whitespace-tolerant, which is the R7
    # finding; and the door token, which is the clause with teeth.
    pattern = re.compile(
        r"\b(?:%s)\s*\(" % names
        + r"|\.append\s*\(\s*start\s*:"
        + r"|\b%s\b" % re.escape(REGION_FILL_DOOR_TOKEN)
        + r"|\bRegionFillDoor\b"
    )
    for path in sorted((ROOT / "Playhead").rglob("*.swift")):
        rel = path.relative_to(ROOT).as_posix()
        if rel in REGION_PRODUCERS:
            continue
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            stripped = line.strip()
            if stripped.startswith("//") or stripped.startswith("///"):
                continue
            if pattern.search(line):
                faults.append(
                    "%s:%d assembles a coverage region outside %s — a region "
                    "built from loose numbers carries whatever population the "
                    "author reached for (probes PJ1 / PK1): %s"
                    % (rel, lineno, " / ".join(sorted(REGION_PRODUCERS)), stripped)
                )

    # The obligation is only worth anything while the doors actually demand it.
    # Deleting a `door:` parameter would make every fabrication legal again AND
    # silence this check, so the check verifies its own premise.
    quantities = (ROOT / QUANTITIES).read_text(encoding="utf-8")
    doors = quantities.count("door: RegionFillDoor")
    if doors != REGION_FILL_DOOR_COUNT:
        faults.append(
            "%d region entry point(s) take a `door: RegionFillDoor`, expected %d "
            "— a door was removed, which re-opens probe PK1 and silences the "
            "clause above at the same time" % (doors, REGION_FILL_DOOR_COUNT)
        )
    return faults


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--list", action="store_true")
    parser.add_argument("--only", default=None)
    parser.add_argument("--check-inventory", action="store_true",
                        help="run the buildless inventory preflight and stop")
    args = parser.parse_args(argv)

    faults = check_inventory()
    if faults:
        sys.stderr.write("untypeable-battery: the buildless preflight failed.\n")
        for fault in faults:
            sys.stderr.write("    %s\n" % fault)
        sys.stderr.write(
            "A PROMOTION fault: add the case, or route the site through "
            "EpisodeSeconds.unsoundPlanListPromotion(_:site:).\n"
            "A FABRICATION fault: get the region from AnalysisStore instead of "
            "building one — a region assembled from loose numbers carries "
            "whatever population was in reach, which is probe PJ1 (rails "
            "TY32/TY34 reproduced one layer below themselves).\n")
        return 2
    print("=== inventory: every promotion site is tagged, only tagged sites "
          "promote, and no file outside the store fabricates a region ===")
    if args.check_inventory:
        return 0

    selected = [m for m in MUTATIONS if args.only in (None, m[0])]
    if not selected:
        sys.stderr.write("no mutation named %r\n" % args.only)
        return 2

    # THE CONTROL IS NOT OPTIONAL. A selection that omits it used to print
    # "control not run" and exit 0 — a partial run reporting success with
    # nothing holding it up, which is the vacuity this battery exists to
    # replace. Appending it costs one build and buys the only evidence that a
    # verdict of UNTYPEABLE means anything at all.
    if not any(m[0] in EXPECT_COMPILES for m in selected):
        selected += [m for m in MUTATIONS if m[0] in EXPECT_COMPILES]

    if args.list:
        for name, path, desc, _, _, fragments in selected:
            print("%-6s %-52s %s" % (name, path, desc))
            if fragments:
                print("%-6s %-52s must diagnose: %s" % ("", "", " + ".join(fragments)))
        return 0

    files = sorted({m[1] for m in selected})
    originals = {f: (ROOT / f).read_bytes() for f in files}
    before = {f: sha(ROOT / f) for f in files}

    # Anchors first, ALL of them, before a single build. A drifted anchor found
    # halfway through costs a rebuild for nothing; found here it costs nothing.
    drift = []
    for name, rel, _, old, _, _ in selected:
        found = (ROOT / rel).read_text(encoding="utf-8").count(old)
        if found != 1:
            drift.append("    %-6s %s: anchor matched %d times, expected 1"
                         % (name, rel, found))
    if drift:
        sys.stderr.write("untypeable-battery: anchor drift — the source moved on.\n")
        sys.stderr.write("\n".join(drift) + "\n")
        sys.stderr.write("Rewrite the EDIT, never the expectation.\n")
        return 2
    print("=== anchors: %d/%d match exactly once ===" % (len(selected), len(selected)))

    # The free unmutated pass. A tree that does not build would make every rail
    # below report UNTYPEABLE against working code.
    print("=== baseline: the UNMUTATED tree must build ===")
    started = time.monotonic()
    rc, out = run_build()
    if rc != 0:
        sys.stderr.write("\n".join(diagnostics(out)[:20]) + "\n")
        sys.stderr.write(
            "\nuntypeable-battery: the tree does not compile before any mutation. "
            "Every verdict below would be meaningless — fix that first.\n"
        )
        return 2
    print("  builds clean in %.0fs\n" % (time.monotonic() - started))

    results = []
    try:
        for name, rel, desc, old, new, fragments in selected:
            path = ROOT / rel
            text = path.read_text(encoding="utf-8")
            if text.count(old) != 1:
                results.append((name, "ERROR", "anchor drift mid-run", []))
                print("%-6s ERROR" % name)
                continue
            path.write_text(text.replace(old, new), encoding="utf-8")
            started = time.monotonic()
            rc, out = run_build()
            elapsed = time.monotonic() - started
            path.write_bytes(originals[rel])
            if sha(path) != before[rel]:
                results.append((name, "ERROR", "restore was not byte-exact", []))
                break

            diags = diagnostics(out)
            compiled = rc == 0
            if name in EXPECT_COMPILES:
                verdict = "OK-COMPILED" if compiled else "CONTROL-FAILED"
            elif compiled:
                verdict = "SURVIVED"
            else:
                blob = "\n".join(diags)
                missing = [f for f in fragments if f not in blob]
                verdict = "UNTYPEABLE" if not missing else "WRONG-ERROR"
            results.append((name, verdict, desc, diags[:3]))
            print("%-6s %-14s (%3.0fs) %s" % (name, verdict, elapsed, desc.split(":")[0]))
            if verdict == "WRONG-ERROR":
                for line in diags[:3]:
                    print("         ! %s" % line)
    finally:
        for rel in files:
            (ROOT / rel).write_bytes(originals[rel])

    for rel in files:
        if sha(ROOT / rel) != before[rel]:
            sys.stderr.write("TREE NOT RESTORED: %s — inspect before anything else\n" % rel)
            return 4

    bad = [r for r in results if r[1] in ("SURVIVED", "ERROR", "WRONG-ERROR", "CONTROL-FAILED")]
    control = next((r[1] for r in results if r[0] in EXPECT_COMPILES), None)
    print("\n%d mutation(s): %d untypeable, %d survived, %d wrong-error, control %s"
          % (len(results),
             sum(1 for r in results if r[1] == "UNTYPEABLE"),
             sum(1 for r in results if r[1] == "SURVIVED"),
             sum(1 for r in results if r[1] == "WRONG-ERROR"),
             control or "NOT RUN"))
    if control is None:
        sys.stderr.write(
            "\nNO VACUITY CONTROL RAN. Every UNTYPEABLE above is unbacked — a "
            "harness that fails unconditionally reports exactly the same thing. "
            "This is a harness bug, not a result; do not read the verdicts.\n"
        )
        return 3
    if bad:
        sys.stderr.write(
            "\nA SURVIVOR HERE IS A TYPE HOLE, not merely a coverage hole: the "
            "substitution can be written and the compiler accepts it, which is "
            "the state this bead exists to leave behind. Widen the types — do not "
            "relax the expectation.\n"
        )
        for name, verdict, detail, diags in bad:
            sys.stderr.write("  %-6s %-14s %s\n" % (name, verdict, detail))
            for line in diags:
                sys.stderr.write("           %s\n" % line)
        return 1
    print("Every substitution fails to compile, and the vacuity control still builds.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
