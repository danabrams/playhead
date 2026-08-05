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
`EpisodeSeconds(` construction in the runner (which is how R2 probe PR5 wrote a
sixth promotion with the enum untouched). It is a LEXICAL tripwire on one file
and can be out-spelled like any other; see limit L-F in `CoverageQuantities.swift`.

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
import subprocess
import sys
import time

ROOT = pathlib.Path(__file__).resolve().parents[1]

STORE = "Playhead/Persistence/AnalysisStore/AnalysisStore.swift"
RUNNER = "Playhead/Services/AdDetection/BackfillJobRunner.swift"
CLAIM = "Playhead/Services/AdDetection/SemanticScanClaim.swift"
ACTIVITY = "Playhead/Services/Activity/ActivitySnapshotProvider.swift"
QUANTITIES = "Playhead/Persistence/AnalysisStore/CoverageQuantities.swift"

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
        "                        upTo: AnalysisCoverageMath.adScanBridgeableGapSec",
        "                        upTo: RescanThresholdSec.adScanRescanWorthyGapSec",
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
                durationSec: durationSec
            )
            // Download fraction comes from the (already-snapshotted)""",
        """            let analysisFraction = fraction(
                area: summary?.adScanCoveredSec,
                durationSec: durationSec
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
                durationSec: durationSec
            )
            let fastTranscriptWatermarkSec""",
        """            let analysisFraction = fraction(
                area: summary?.fastTranscriptCoveredSec,
                durationSec: durationSec
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

    # `EpisodeSeconds.` (the two named promotions) is fine; `EpisodeSeconds(`
    # is a raw construction and is the bypass L-F describes.
    raw = runner.count("EpisodeSeconds(")
    if raw:
        faults.append("%d bare `EpisodeSeconds(` construction(s) in %s — a "
                      "PlanList→Episode promotion that no site tag records"
                      % (raw, RUNNER))
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
        sys.stderr.write("untypeable-battery: the unsound-promotion inventory does "
                         "not match the sites.\n")
        for fault in faults:
            sys.stderr.write("    %s\n" % fault)
        sys.stderr.write("Add the case, or route the site through "
                         "EpisodeSeconds.unsoundPlanListPromotion(_:site:).\n")
        return 2
    print("=== inventory: every promotion site is tagged, and only tagged sites promote ===")
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
