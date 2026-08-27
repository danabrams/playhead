# playhead-9s1z — option (iii) measured

**MEASUREMENT ONLY. Dan has chosen no option. Nothing on this branch changes shipped
behaviour**, and the one source edit (`SemanticSweepMarkComposer.versionScopePolicy9s1z`)
defaults to `.shipped`, which is what every production call site takes.

Pull: `/Users/dabrams/playhead-gate-artifacts/device-pulls/2026-08-19-t4-correction/work/analysis.sqlite`
(read-only; `db/` never touched). Tree: `bead/playhead-9s1z` off main `2443e2ce`.
Regenerate with `tools/9s1z/build.sh && tools/9s1z/recompose > tools/9s1z/out/t4-recompose.json`,
then `python3 tools/9s1z/report.py`.

## Method — the composer, not a model of it

playhead-kg6i's figures came from a **re-implementation** of the composer. These come from
**the composer**: `tools/9s1z` compiles `SemanticSweepMarkComposer.swift` verbatim from the app
target into a macOS host binary. `extract-deps.sh` copies only the supporting *data*
declarations, by line range; `verify-deps.sh` re-extracts and diffs, so a hand edit in the
"verbatim" file fails loudly.

`compose(supportLines: nil)` throughout, which is what `BackfillJobRunner` passes. The
`AdDetectionService` site passes a real `SupportLineIndex` — see "Why the nil index does not
bound this result" below for why that cannot change the (iii) conclusion.

## The three options

**(i)** today — a coarse `containsAd` window is narrowed by ANY overlapping `passB` refinement,
whatever transcript that refinement was formed against.
**(ii)** require the same `transcriptVersion`. The pairing is refused, so the coarse window
stands UN-NARROWED, and the refinement — now unclaimed — stands alone as its own extent.
**(iii)** require the same version and keep BOTH halves of the refused pairing out of the mark
set: the coarse window contributes nothing rather than widening back to its full tile, and a
refinement whose only overlapping coarse windows were all cross-version is suppressed rather
than standing alone. A coarse window with no overlapping refinement at all, and a refinement
inside no coarse window at all, are untouched — neither is a cross-version case.

## THE TABLE (this tree, stage 6 ON)

| option | marks | total marked seconds | inner-edge seconds added | outer-edge seconds added | seconds removed | net |
|---|---|---|---|---|---|---|
| (i) today | **78** | **7224.0** | — | — | — | — |
| (ii) same version | **77** | **7273.5** | **+96.0** | **+0.0** | **−46.5** | +49.5 |
| (iii) same version, drop both | **78** | **7224.0** | **+0.0** | **+0.0** | **−0.0** | **0.0** |

On the stage-6-OFF basis (the basis this bead's own figures are stated over — see below), the
same three rows read **79 / 8048.8**, **78 / 8098.3 (+96.0 inner / 0 outer / −46.5)**, and
**79 / 8048.8 (0 / 0 / 0)**. **The deltas are identical on both bases.** Per-asset tables for
both are in `out/report.txt`.

### INNER vs OUTER — the predicate, stated

An added run is **OUTER** when it abuts the episode head or tail (within 30 s of 0, or within
30 s of `analysis_assets.episodeDurationSec`), i.e. there is no show on the far side of that
edge to lose. Everything else is **INNER**. All three runs (ii) adds are mid-episode — the
nearest episode boundary to any of them is **337 s away** — so **(ii) buys zero outer seconds
and pays 96.0 s of inner ones.** Each run's distance to both boundaries is recorded in the JSON
so the classification is auditable rather than asserted.

## What each option does to the three worked examples

| | (i) today | (ii) | (iii) |
|---|---|---|---|
| `7DD870DC` A | `[726.48–738.30]` 11.8 s | *gone* — replaced by `[702.06–789.18]` 87.1 s | `[726.48–738.30]` 11.8 s |
| `7DD870DC` B | `[3168.96–3215.46]` 46.5 s | **VANISHES — no mark at all** | `[3168.96–3215.46]` 46.5 s |
| `561CEF5B` | `[1517.64–1594.32]` 76.7 s | *gone* — replaced by `[1517.64–1615.02]` 97.4 s | `[1517.64–1594.32]` 76.7 s |

### The 96.0 s (ii) adds is show, verbatim from `transcript_chunks`

* `7DD870DC` **+50.9 s** after 738.30 — *"Leslie, last time, you were on the pod. You said, my
  name's Lizzie Jones, and I couldn't give a shit if I was Conan's friend."* The interview.
* `7DD870DC` **+24.4 s** before 726.48 — *"Just to see if they laugh… I did for 28 years on
  television. I sat and watched people watch me."* Also the interview.
* `561CEF5B` **+20.7 s** after 1594.32 — *"Well, outside of pharmacology, if we don't want to
  take a medication… manage stress, manage cortisol… all of the things that Mother Nature gave
  us."* The medical content of the show.

The `passB` refinement's edges are, in both cases, right where the promotional content stops.

### THE −1 MARK UNDER (ii) IS A REAL AD

`7DD870DC [3168.96–3215.46]` is not narrowed, merged or replaced under (ii). It **disappears**,
and 46.5 s of audio that carries a mark today carries none. Its transcript:

> *"…all American summer starts with an all American beer and what's that beer? It's Miller.
> Right, go to merolite.com slash Conor to find delivery options near you. Do it now. Or you can
> pick up some mill light pretty much anywhere they sell beer. It's Miller time. Celebrate
> responsibly. Miller Brewing Company, Milwaukee, Wisconsin. 96 calories…"*

Sponsor, URL, call to action, and the legal boilerplate. About as unambiguous as an ad gets.

**Mechanism, traced stage by stage** (`stageTraces.7DD870DC` in the JSON). Un-narrowed, the
coarse tile stands at `[3158.52–3229.68]`; stage 3 merges it with `[3081.84–3157.74]` across a
0.78 s gap into `[3081.84–3229.68]`; stage 5 then blocks that whole 147.8 s extent because
`detection-v1 [3076.10–3085.90]` overlaps it. That is the documented "one stray narrow window
anywhere inside a coarse verdict suppresses the whole mark" loss the composer's own header
pins — and **widening the extent is what puts the mark within reach of it.** (i) and (iii)
never widen, so the extent stays clear of the blocker and the mark survives.

## WHAT (iii) LOSES: nothing, on this pull

**0 marks and 0.0 seconds**, under both stage-6 configurations, with byte-identical bounds on
all 15 assets. **No mark disappears, so the "are any of them real ads" question is vacuous
here** — the population is empty. Contrast (ii), where it is not.

That is not because the rule sat idle. Under (iii) it **suppresses 16 coarse windows** and drops
**65 of 371 presence extents (17.5 %)**. The difference is absorbed downstream:

* (i) and (iii) **diverge at stage 1–2 on 5 of the 15 assets** and **re-converge at stage 3
  (merge) on all 15** — asserted mechanically by `report.py`, which exits non-zero if any asset's
  `merged` / `afterClipAndWidth` / `marks` sets differ.
* **11 of the 16** suppressed coarse windows have a coarse presence row with **identical bounds
  at the refinement's own `transcriptVersion`**, so the same narrowing is produced by a
  same-version sibling and nothing is lost.
* The other **5** (`AA6CD430` ×2, `C0610BF9` ×3) sit under audio that yields no mark under (i)
  either. `C0610BF9` 884–1011 s is the worked case: both options merge to `[900.18–1011.66]`,
  and stage 5 blocks it on `detection-v1 [884.77–914.83]` and a `userCorrection` window.

### The zero is a measurement, not a dead branch

A rule that never fires and a rule that fires and costs nothing are indistinguishable from a
zero. `reachabilitySelfTest` in the JSON makes all three options disagree on synthetic rows —
one coarse tile at `V1` `[100,200]`, one `passB` refinement at `V2` `[120,140]`, nothing else:

| option | marks | suppressed coarse | suppressed orphans |
|---|---|---|---|
| (i) | `[120.0–140.0]` | 0 | 0 |
| (ii) | `[100.0–200.0]` | 0 | 0 |
| (iii) | *none* | **1** | **1** |

So both of (iii)'s suppressions bite when the shape occurs. **The orphan half fires 0 times on
the pull** — every `passB` refinement there has at least one same-version coarse window over it,
which is expected, since a refinement plan is built from a coarse row's own `supportLineRefs`.

### Why the nil index does not bound this result

Stage 6 (`localise`) runs AFTER the stage-5 dedupe and is a pure function of
`(surviving extent, scan rows, supportLines)`. The surviving extents are **identical** between
(i) and (iii) on every asset, so stage 6 receives identical input and must return identical
output **for any `SupportLineIndex`** — including the real one `AdDetectionService` passes. The
`supportLines: nil` choice therefore does not limit the (iii) conclusion. It does affect the
absolute totals, which is the next section.

## Did the bead's figures reproduce?

| figure in the bead | reproduced? | on what |
|---|---|---|
| 65 coarse × passB pairings cross transcript versions | **YES, exactly** | 65 of **133** overlap pairings |
| 27 of 79 marks rest on presence rows from >1 version (22 two / 4 three / 1 five) | **YES, exactly** | 52+22+4+1 = 79, stage-6-OFF basis |
| (ii) 78 marks / 8098.3 s | **YES, exactly** | stage-6-OFF basis only |
| (i) 79 marks / 8048.8 s | **YES, exactly** | stage-6-OFF basis only |
| (i) on the code in THIS tree | **NO — it is 78 marks / 7224.0 s** | stage 6 ON |

**The bead's baseline is stale, and the cause is bisected.** Disabling stage 6 reproduces every
one of kg6i's stated numbers to the digit — including its fidelity claim (*"77 of the 79
recomposed extents match a persisted `ad_windows` row exactly"* → **77 of 79** here) and its
entire per-asset table, row for row. So those figures were taken from a model that did not run
stage 6. Recomposing at each composer commit since:

| composer commit | marks | seconds |
|---|---|---|
| `0f0e9cb1` playhead-kg6i itself | 80 | 7491.5 |
| `83de34c8` playhead-my33 (the `.absent` sole-backing rule) | **78** | **7224.0** |
| `bab3ce60` playhead-iw7q | 78 | 7224.0 |
| `3eff997e` playhead-vz3l | 78 | 7224.0 |

`playhead-my33` moved it; `iw7q` and `vz3l` did not. **The delta this bead is about is
unaffected** — −1 mark and +49.5 s on either basis, same three worked examples, same
inner/outer split.

With stage 6 ON, only **63 of 78** recomposed marks have an exact persisted twin. That is
expected and is not a fidelity problem: the device wrote those 82 `semantic-sweep-v1` rows with
a build that predates `shu5`/`my33`, so a recompose on today's geometry is *supposed* to differ.
The stage-6-OFF run is the apples-to-apples fidelity check, and it lands at 77 of 79.

## Naming the populations, because three neighbours have been confused here before

None of the figures above is any of the three quantities kg6i had to separate. For the record:

* **211 of 301** — coarse `passA` `containsAd` rows whose `transcriptVersion` is not the one the
  asset's current canonical chunk set hashes to. *Rows at a superseded version.* Not used here.
* **280 of 301** — rows whose `transcriptVersion` no surviving `transcript_chunks` row carries.
  *Rows whose chunks are gone.* Refuted by kg6i; not used here.
* **130 of 301** — playhead-shu5's localisation bucket: *rows this stage cannot resolve.* Not
  used here.

What this bead's figures count, numerator and denominator both:

* **65 of 133** — **PAIRINGS, not rows.** The denominator is every (coarse presence row, `passB`
  presence row) pair on one asset whose windows OVERLAP; the numerator is those whose two rows
  carry different `transcriptVersion`s. One row can appear in many pairings, so this is not a
  row count and must not be read against 301.
* **16 of 301** — coarse `passA` `containsAd` presence rows that have at least one overlapping
  `passB` presence row **and** for which EVERY such refinement is at a different version. This
  is the population (ii) widens and (iii) suppresses.
* **65 of 371** — **presence EXTENTS**, the output of stage 1–2 across all 15 assets, dropped by
  (iii). Extents, not rows: one row can produce several.
* **27 of 79** — marks (stage-6-OFF basis) over which the overlapping presence rows carry more
  than one distinct `transcriptVersion`. Denominator is marks, not rows.
* **78 / 7224.0 s** and **79 / 8048.8 s** — marks and summed mark duration over all 15 assets of
  the one pull, at the two stage-6 configurations.

## Verification status

* `scripts/lint.sh` — **clean** (SwiftLint strict + SHAPE-2 preflight, 6 actor slots all
  allowlisted).
* `tools/9s1z/verify-deps.sh` — **clean**: `Deps.swift` is a verbatim extract.
* Host build of the patched composer — **clean** (the harness compiles the same source the app
  target does).
* **NO TEST VERDICT.** A scoped `-only-testing:` run over the 17 sweep/support-line suites was
  started and then **deliberately abandoned mid-build** because a second session's mutation
  battery was building concurrently in `.worktrees/qjcf` — the documented two-builds-on-16 GB
  OOM shape, where the loser dies with `** BUILD INTERRUPTED **` and no test failure. Only this
  worktree's `xcodebuild` (pid 80739) was terminated; the other session's (96472) was left
  alone. **The `pgrep` check is weaker than it looks and is stated precisely rather than
  claimed:** `pgrep -x xcodebuild` was run immediately before the FIRST launch and reported
  none. That launch hit a 10-minute foreground timeout during the fresh worktree's cold build
  and was relaunched in the background — and `pgrep` was **not** re-run before the relaunch. So
  "the check passed" is true of a moment that is not the moment the surviving process started,
  which is the same defect class this bead is about. Whether the qjcf run began before or after
  the relaunch is **not established here**. **An abandoned run is not a pass** — the suite still
  owes a verdict if any of this is ever taken further than a measurement.

## Recommendation — a recommendation, not a decision

On this evidence **(iii) dominates**: it applies the same reasoning kg6i applied to the votes
("a refinement of a transcript this window was never scanned against is not this window's
narrowing"), gives up not one mark and not one second of reach on this pull, and structurally
cannot produce (ii)'s failure mode, where widening an extent walks it into an existing window
and deletes a fully-scripted Miller Lite host-read. (ii), measured, buys nothing: all 96.0 s it
adds is show at inner edges, none of it outer, and it costs a real ad.

The caveat to weigh before reading "free" as a property rather than as a reading of one pull:
(iii)'s zero cost is **contingent on replication**. It holds because 11 of the 16 affected
coarse tiles have a same-bounds screening at the refinement's own version, and the other five
sit under audio that is blocked anyway. On an episode where a refinement's version has no coarse
screening of its own, (iii) removes a mark (i) would have kept — exactly what the synthetic case
shows. So the honest framing is: **(iii) is free today and is reach-negative in the general
case; (ii) is reach-negative today and pays show at every inner edge.** Which risk is
preferable, and whether the reach exposure is worth the correctness, is Dan's call.
