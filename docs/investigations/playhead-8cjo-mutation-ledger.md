# playhead-8cjo — mutation ledger

**A KILLED column is not evidence.** What makes a mutant evidence is that the set
of tests it reddens is the set somebody wrote down BEFORE running it. A mutant
that reports KILLED while killing a *different* set is a false credit, and it is
the reason this file exists as a record rather than as a count.

Predictions were written to
`/Users/dabrams/playhead-gate-artifacts/8cjo/MUTATION-PREDICTIONS.md` before the
first mutant was applied; the observed column below is transcribed from
`scripts/mutation-battery.sh`'s own `observed failures:` block.

Run protocol: one mutant per invocation (`--only <NAME>`), so every verdict is
attributable to one edit. `AK01` and the two vacuity controls carry the battery's
own green-baseline check; the middle runs used `PLAYHEAD_MB_SKIP_BASELINE=1`
because `restore_and_verify` re-hashes every mutable file between runs and
`git status --porcelain -- Playhead` was re-read after each.

## The AK series — "a host is attached" is not "a card was shown"

| mutant | what it re-introduces | predicted | observed | verdict |
|---|---|---|---|---|
| AK01 | playhead-2d6i's shipped behaviour: the receipt is written only when NOBODY IS SUBSCRIBED | PARTITION, ACCEPTED, REFUSED, UNFORWARDED, QVETO, WALK (6) | **the same 6, exactly** | KILLED |
| AK02 | the `didAccept` guard pushed into the SUGGEST arm, so the auto tier acknowledges an item the queue refused | REFUSED, WALK, QVETO, GUARD (4) | the same 4, **plus one unrelated flake** (see below) | KILLED |
| AK03 | the auto tier's acknowledgement deleted | ACCEPTED, WALK, ACKAUTO (3) | **the same 3, exactly** | KILLED |
| AK04 | the seam stops checking the EPISODE | SEAM (1) | **SEAM, exactly** | KILLED |
| AK05 | the seam stops checking the PLAYBACK GENERATION | SEAM (1) | **SEAM, exactly** | KILLED |
| AK06 | the seam stops checking the MATERIAL TOKEN | SEAM (1) | **SEAM, exactly** | KILLED |
| AK07 | the delivered record is written BEFORE the guard, so the seam manufactures a card for a window nothing announced | SEAM (1) | **SEAM, exactly** | KILLED |
| AK08 | endEpisode stops clearing the delivered-card record | CLEARS (1) | **CLEARS, exactly** | KILLED |
| AK09 | beginEpisode stops clearing it | CLEARS (1) | **CLEARS, exactly** | KILLED |
| AK10 | the emit path stops gating on a subscriber | PARTITION, **ATTACH** (2) | **the same 2, exactly — including the widened half** | KILLED |
| AK11 | the view model keeps its own copy of the forwarding rule | DELEGATES, NOCOPY (2) | *running* | |
| AK12 | the acknowledgement records no delivery | ACCEPTED, PARTITION, WALK, SEAM, CLEARS (5) | | |
| AK99 | **VACUITY CONTROL** — the seam's receipt binding renamed, nothing else | **MUST SURVIVE, 0 failures** | | |

Re-aimed from playhead-2d6i in the same change: MS01 (re-anchored, and widened
from "the unattached arm records nothing" to "an announced skip records nothing
at all"), MS02 (**re-aimed** — its old body is now the shipped behaviour, so it
would have been an equivalent mutant scored as a kill; the DOUBLE DELIVERY
direction it has always owned moved to the seam), MS06/MS15 (re-anchored around
the new clear), MS07 (re-anchored), MS99 (its second anchor re-anchored).

### Two things this ledger exists to record, because a table would hide them

**AK07 killed exactly its predicted victim, and that victim is a test nothing
else in the tree can reach.** `AK04`–`AK07` are single-victim by construction:
the production caller always passes the announced card's own fields, so no
behavioural suite can ever exercise a wrong episode, a wrong generation, a wrong
material token, or an unknown window. That is precisely why the seam's contract
is pinned directly by
`MissedAutoSkipReceiptListTests.theSeamRefusesAnIdentityThatIsNotTheAnnouncedCards`
rather than inferred from the queue tests. A single-victim mutant is the strongest
shape available here — it says the rail is the *only* thing standing there.

**AK10's prediction was WIDENED before the run, not after it.** The first version
named only `THE EXTENDED PROPERTY`. Re-reading
`attachingAHostLaterNeitherCardsNorDuplicatesAMissedReceipt` showed it drives an
unattended skip and then asserts `emittedAutoSkipBannersSnapshot()` is empty — so
removing the emit path's subscriber gate reddens it too. The record and the
prediction file were both updated and committed (`321b9339`, then the AK10 fix)
**before** the battery reached that batch. A prediction corrected after the
observation is not a prediction.

### The one observed failure nobody predicted, and why it is not a finding

AK02's run also reddened
`closing the session log is non-terminal — a later write reopens the SAME file`
(`PlayheadTests/Services/Diagnostics/RuntimeStoreTeardownTests.swift:220`). That
suite is in `FOCUSED_SUITES` for **playhead-882eg**, it constructs a whole
`PlayheadRuntime` and touches the filesystem under a 3-minute time limit, and no
mutation of the banner-delivery path can reach it. It did not appear on AK01's
run (which carried a green baseline) nor on AK03–AK07. Treated as a flake and
re-verified scoped before the merge gate; it is **not** in
`scripts/gate-baseline.PlayheadFastTests.json`, so if it recurs it belongs to
882eg's suites rather than to this bead.

## A residual that is unreachable BY TYPE, recorded as such

A reviewer suggested adding `receipt.item.analysisAssetId != nil` to
`missedAutoSkipReceipts()`'s read-time filter, on the ground that
`denyAutoSkippedBanner` requires a non-nil asset id and a row it would refuse is
"a button that does nothing".

**It cannot happen, and the type is the witness.** `AdSkipBannerItem
.analysisAssetId` is `String?`, but `emitBannerItem` fills it from
`adWindow.analysisAssetId`, and `AdWindow.analysisAssetId` is a non-optional
`String` (`Playhead/Persistence/AnalysisStore/AnalysisStore.swift`). Every auto
tier item therefore carries a non-nil asset id on every path into the receipt
dictionary. Adding the clause would be a branch no input can enter, and a branch
nothing can reach is a branch no test can cover — it would read as defensive
depth while being dead code.

Recorded as **unreachable-by-type**, not as fixed. If `AdWindow.analysisAssetId`
is ever made optional, this paragraph is the thing that has to be re-read.

## Pre-run audit: does any remaining mutant delete the CHECK instead of the DEFECT?

A mutant that removes the thing that DETECTS a defect, rather than re-creating
the defect, proves nothing — it can only ever kill the test that was watching it.
The previous bead lost a round to exactly that (`FD06` v1 survived and had to be
re-aimed), so the ten mutants still to run were audited against the shape
**before** their verdicts landed, not after.

| mutant | mutates | re-creates a defect, or deletes a check? |
|---|---|---|
| AK10 | production `guard hasAttachedHost else { return }` | a defect — but see the caveat below |
| AK11 | production `observeBanners` body | a defect: the view model enqueues and never acknowledges, which is the auto tier's original state |
| AK12 | the seam's `deliveredAutoSkipCardWindowIds.insert` | a defect: a card the listener saw becomes indistinguishable from a skip nobody announced |
| MS01 | the receipt write | a defect: playhead-2d6i verbatim |
| MS02 | the seam's receipt REMOVAL | a defect: one skip on two surfaces |
| MS06 / MS15 | the two per-episode clears | a defect: a leak into the next episode |
| MS07 | `playheadTimeAtSkip: currentPlayheadTime` | a defect: the SPAN's start substituted for the listener's position — the standing class |
| AK99 / MS99 | a local rename | neither: vacuity controls, MUST SURVIVE |

**The caveat on AK10, stated rather than buried.** It mutates a production line,
so it is not the FD06 shape — but that line's only observable effect is on
`emittedAutoSkipBannerWindowIds`, which is test-only observability, and the two
`for … in continuations` loops it also guards iterate zero times when nobody is
subscribed. So AK10 is a rail on an OBSERVABILITY invariant, not on behaviour a
listener could notice. That is a weaker claim than the other eleven make, and it
is worth having anyway: the yield set is what
`cards.isSubset(of: yielded)` and the `.subscribedButNeverAcknowledging`
non-vacuity check are measured against, so a yield set that lies makes those two
rails lie with it.

## How a SURVIVED verdict is verified

`scripts/mutation-battery.sh` reads a test with NO VERDICT as a PASS
(`playhead-gjlp0`, open), so a crash-looping batch prints a FALSE SURVIVED and
zero observed failures — indistinguishable on the results table from a genuine
vacuity control. A SURVIVED verdict is therefore not accepted on the table
alone: the batch's own xcodebuild log (kept under
`/private/tmp/playhead-mutation-battery.*`) must show the focused suites
actually RAN, i.e. a Swift Testing summary line with a test count in the tens.
If the control ever dies, every KILLED above it is void, because they would all
have been scored against a tree that fails for a reason unrelated to the mutant.
