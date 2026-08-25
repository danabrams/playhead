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
| AK10 | the emit path stops gating on a subscriber | PARTITION, **ATTACH** (2) | *running* | |
| AK11 | the view model keeps its own copy of the forwarding rule | DELEGATES, NOCOPY (2) | | |
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
