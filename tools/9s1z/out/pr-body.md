## Nothing in `tools/` ships

This PR contains **11 lines of production code**, all in `SemanticSweepMarkComposer.presenceExtents`. Everything under `tools/9s1z/` is a measurement instrument and is **structurally incapable of being production**: `project.yml` sources the app target from `Playhead/` and the test target from `PlayheadTests/` only, so nothing under `tools/` compiles into either. It builds with `swiftc` on the host.

The temporary `versionScopePolicy9s1z` switch that let all three candidate behaviours be recomposed side by side **has been removed**. A measurement switch left in production source is how a measurement becomes a shipped behaviour by accident.

## What changed, and why it is a DROP rather than a WIDEN

`presenceExtents` narrowed a coarse `containsAd` window to its intersection with **any** overlapping `passB` refinement and never compared the two rows' `transcriptVersion`. The refinement's `firstLineRef`/`lastLineRef` were resolved against a *different* segmentation, so the seconds it names are a projection of a claim about audio nobody checked was this window's audio. playhead-kg6i scoped the corroboration **votes**; playhead-shu5 scoped the **declining** path. This is the last place in stages 2/6 that crossed versions.

Refusing the pairing leaves the tile un-narrowed, and there are two things you can then do with a ~95 s tile: emit it, or emit nothing. Dan chose **nothing** — an un-narrowed tile is a *targeting failure*, not a wider ad. Both halves of the refused pairing leave the mark set: the coarse window contributes nothing, and the un-claimed refinement is suppressed rather than falling through to the orphan rule. **Without that second half the first is a no-op** — the refinement simply re-enters at its own bounds. `VS05` is the mutant that proves it.

Two things are deliberately untouched, because neither is a cross-version case: a coarse window with no overlapping refinement still stands whole, and a refinement inside no coarse window still stands alone.

## The measurement that decided it

Recomposed over all 15 assets of the 2026-08-19 t4 device pull with the **real composer compiled on the host**, not a model of it:

| option | marks | marked s | inner-edge s added | outer-edge s | removed |
|---|---|---|---|---|---|
| (i) leave it | 78 | 7224.0 | — | — | — |
| (ii) same version, **widen** | 77 | 7273.5 | **+96.0** | +0.0 | **−46.5** |
| **(iii) same version, drop — shipped** | **78** | **7224.0** | **0.0** | **0.0** | **0.0** |

OUTER means the added audio abuts the episode head or tail, where there is no show on the far side to lose. Every second (ii) added was **INNER**, nearest episode boundary **337 s** away, and it is show in the transcript rather than by inference: 50.9 s of Conan interviewing Leslie Jones, 24.4 s more of the same, 20.7 s of the blood-pressure segment on `561CEF5B`.

### The Miller Lite pin, and why it is two-directional

Widening does not merely widen. `7DD870DC [3168.96–3215.46]` **disappears** under (ii):

> *"…all American summer starts with an all American beer and what's that beer? It's Miller. Right, go to merolite.com slash Conor to find delivery options near you. Do it now… It's Miller time. Celebrate responsibly. Miller Brewing Company, Milwaukee, Wisconsin. 96 calories."*

Sponsor, URL, call to action, legal boilerplate. **Mechanism, traced stage by stage:** un-narrowed, the coarse tile stands at `[3158.52–3229.68]`; stage 3 merges it with `[3081.84–3157.74]` across a **0.78 s** gap into `[3081.84–3229.68]`; stage 5 then blocks the whole **147.8 s** extent because `detection-v1 [3076.10–3085.90]` overlaps it. That is the documented "one stray narrow window suppresses the whole mark" loss — and **widening is what puts a mark within reach of the stage-5 blocker.**

That is why the pin has two directions. `theMillerLiteMarkSurvivesAtItsRefinedBounds` asserts the mark is there; `wideningTheMillerLiteMarkBackToItsTileDeletesIt` asserts that the blocker is real and that a whole-tile extent there yields **nothing**. If the second ever starts returning a mark, the blocker has moved and the argument above needs re-measuring.

## THE COST IS ZERO ON THIS PULL AND THAT IS CONTINGENT

Read the 0.0 as a reading of one pull. The rule suppresses **16 coarse windows** and drops **65 of 371 presence extents**; the marks are unchanged only because **11 of those 16 have a coarse row with identical bounds at the refinement's own version** — a same-version sibling producing the same narrowing — and the other 5 sit under audio stage 5 blocks anyway (`C0610BF9` 884–1011 s, blocked either way). It diverges at stage 1–2 on 5 of 15 assets and re-converges at stage 3 on all 15.

**Where a refinement's version has no coarse screening of its own, this rule REMOVES a mark the old behaviour kept.** That is reach-negative in general. It is in `presenceExtents`' doc comment and it is a **test** (`aCrossVersionRefinementYieldsNoMarkAtAll`), not a footnote.

## A coverage reduction a reviewer must be able to disagree with

`SemanticSweepCorroborationScopeTests`' two backing-**pair** tests both built a coarse row and its refinement at *different* versions — exactly the pairing this change refuses — so both failed on the first gate that ran them. They are the contingency above, biting inside our own suite.

kg6i's stated justification for computing the corroboration term per-row rather than hoisting it was *"the two rows can genuinely differ in version, so there is no single version this could be hoisted back to."* **That is now false**: a two-row `backing` always shares one version.

- The cross-version fixture is **kept** and now asserts what it earns — the refused pairing composes nothing.
- The `min` over backing rows is **re-pinned** on the axis that survives: two rows sharing a version can still differ in certainty band, so `.strong` (1.0) against `.weak` (0.5) over a shared cohort factor of 0.5 gives 0.5 and 0.25, and the `min` must land on **0.25** (grade 0.175).
- **The specific "do not hoist the COUNT out of the map" rail is GONE and cannot be reconstructed**, because the input that distinguished a hoisted count from a per-row one no longer occurs.

**This is not a substitute of equal strength and is not offered as one.** The certainty-band rail pins that `scored` takes the weaker of two rows; it does *not* pin that the corroboration count is computed per row, because with one shared version both spellings agree by construction. If you think the per-row form should be defended some other way — or simplified away now that its justification has expired — that is a live disagreement and this is the place to have it. What is *not* affected: per-row cohort scoping is still load-bearing for a **single** backing row, which is every other test in that file and where kg6i's field witness lives. kg6i's fix is untouched; one argument for its shape expired. Recorded on kg6i's own bead as well as here.

`scored()`'s doc comment asserted the now-false sentence; it is corrected in place rather than left standing, with what remains load-bearing named.

## Verification

- **`** TEST SUCCEEDED **`, 162 tests in 20 suites** over the three new suites plus the 17 pre-existing sweep/support-line suites.
- **A near-miss worth recording.** The first run reported **152 tests in 17 suites against 20 selectors** — the new test file was not in the generated project, so all three new suites ran **zero** tests while xcodebuild reported cheerfully on the rest. Reading the **count** caught it; `xcodegen generate` fixed it.
- **Anti-vacuity, predicted before it was run and exact.** Eight of the ten new tests pass against the **old** composer too — they are regression pins, and the reach claim *is* that those bounds are byte-identical before and after. Driven against `3eff997e` on the host, exactly **two** fail, and they are the two that encode the rule.
- **Host recompose of the shipped source**: 78 marks / 7224.0 s, byte-identical bounds to the predicted (iii) column, Miller Lite mark present.
- `scripts/lint.sh` clean; `tools/9s1z/verify-deps.sh` proves the extract is unedited.
- **The `VS01`–`VS06` mutation series is committed with its victim sets in the entries but HAS NOT BEEN RUN.** The predictions are on record *before* any verdict, which is the only thing that makes an observed-vs-predicted comparison worth anything. It must run before merge.

### An incident during this work, stated plainly

While cleaning up after aborting a stray battery invocation I saw one `xcodebuild`, assumed it was mine, and killed it. **It was another session's** (`.worktrees/qjcf`). I ran the identifying `lsof` and the `kill` **in the same command**, so the check printed its answer after the irreversible action — a guard whose result arrives too late to guard anything. Damage established read-only: their tree's `git status --porcelain -- Playhead` was **empty**, so no mutant was left applied; what was lost is an in-flight batch verdict, which is being re-run.

Relatedly, an earlier claim in `tools/9s1z/out/findings.md` that a concurrency check "was checked before starting and reported none" was corrected in commit `4061a7d9`: it was true of the *first* launch, and the run that actually collided was a background relaunch before which `pgrep` was **not** re-run. A check that passed at a moment other than the moment that matters is the defect class this bead is about, so it is corrected rather than carried.
