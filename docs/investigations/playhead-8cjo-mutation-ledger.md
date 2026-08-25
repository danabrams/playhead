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
| AK11 | the view model keeps its own copy of the forwarding rule | DELEGATES, NOCOPY (2) | **the same 2, exactly** | KILLED |
| AK12 | the acknowledgement records no delivery | ACCEPTED, PARTITION, WALK, SEAM, CLEARS (5) | **the same 5, exactly** | KILLED |
| AK99 | **VACUITY CONTROL** — the seam's receipt binding renamed, nothing else | **MUST SURVIVE, 0 failures** | **0 failures, 2577 tests passed, TEST SUCCEEDED** | **SURVIVED** |

MS01 verdict: **KILLED, and its eighteen observed victims are the eighteen predicted ones — set equality, no extras and no misses.** It is the widest prediction in the series and the one most able to be sloppy.

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

## AK17 — the bypass a reviewer DEMONSTRATED, and why AK11 cannot settle it

Review produced working code that reintroduces this bead's defect and passes
every assertion of the delegation canary as first written. The canary forbade
the literal `queue.enqueue(`; the bypass renames the parameter:

    func observeBanners(from: …, into bannerQueue: AdBannerQueue, …)
        …
        bannerQueue.enqueue(item, hostGeneration: hostGeneration)

`bannerQueue.enqueue(` contains `Queue.enqueue(` with a CAPITAL Q, so the
lowercase forbidden substring does not match. One character of case is the
entire bypass. The auto tier is then never acknowledged, every receipt becomes a
permanent row for cards that WERE shown, and every behavioural suite stays green
because they drive `BannerHostDelivery.forward` directly.

**AK11 cannot prove the fix for this, and that is the part worth recording.**
AK11's replacement body keeps the parameter named `queue`, so it is killed by
exactly the substring the bypass renames. A canary strengthened against the
bypass would report green under AK11 without ever exercising the property it now
guards — a LOST rail rather than a passing one, which is the same shape as the
previous bead's `FD06` v1 (an edit that removed the check instead of
reintroducing the defect, scored as a survivor and re-aimed).

So the canary is scoped to `observeBanners`' own brace-balanced body and forbids
`.enqueue(` on the SELECTOR, and **AK17 re-creates the bypass verbatim, rename
included**. The two mutants are kept distinct because their victim sets differ,
and the difference IS the strengthening:

| mutant | body keeps a `forward(` call? | predicted victims |
|---|---|---|
| AK11 | no — the whole call is replaced | DELEGATES **and** NOCOPY |
| AK17 | yes, in an `else` branch for retirements | **NOCOPY only** |

If AK17 SURVIVES, the strengthening is not real and that is the finding to
report rather than a green row. The most likely way it survives is a brace scan
that starts at the wrong `{` and returns the whole file — which would re-create
the file-wide check under a new name and pass for the same wrong reason.

### The brace scan was validated before it was written, in both directions

The ledger names one way AK17 could survive: a scan that starts at the wrong
`{` and silently returns the whole file, re-creating the file-wide check under a
new name. That risk was measured rather than reasoned about — the algorithm was
run over the REAL `NowPlayingViewModel.swift` in both states, which the battery
made available for free by having AK11 applied at the time:

| tree state | body extracted | `BannerHostDelivery.forward(` in body | `.enqueue(` in body |
|---|---|---|---|
| HEAD, unmutated | 627 chars of a 14,309-char file | **yes** | no |
| AK11 applied | 623 chars of a 14,305-char file | **no** | **yes** |

So the scan discriminates, and the canary passes on the real code and fails on
the real defect.

**It also turned up a defect in the drafted fix, which is why it was worth
running.** At HEAD the FILE contains both `.enqueue(` and `acknowledge` outside
`observeBanners` — in `observeBanners`' own doc comment and elsewhere in the
type. A file-wide ban on those two spellings would have been RED on correct
code from the first commit. Body-scoping is not a refinement of the check here;
it is what makes the check possible at all.

**Read HEAD, not the working tree, while a battery is running.** Both reviewers
independently hit this and said so; the first pass of this validation scanned
the working tree and read AK11's mutant as the shipped code. `git show HEAD:<path>`
is the whole remedy.

### All twelve behavioural AK mutants are in, and every observed set is the predicted set

AK01–AK12: twelve mutants, twelve KILLED, and in every single case the tests
that went red are exactly the tests the prediction named — no mutant killed a
test it did not name, and none failed to kill one it did. The victim counts run
6, 5, 3, 1, 1, 1, 1, 1, 1, 2, 2, 5; the four single-victim ones (AK04–AK07) are
the seam's identity clauses, which no behavioural suite can reach.

One observed failure across the whole series was NOT predicted, and it is not
this bead's: AK02's run also reddened `closing the session log is non-terminal —
a later write reopens the SAME file`
(`PlayheadTests/Services/Diagnostics/RuntimeStoreTeardownTests.swift:220`), a
playhead-882eg suite that constructs a whole `PlayheadRuntime` and touches the
filesystem. It appeared once in nineteen runs, on a mutation of the banner
delivery path that cannot reach it, and not on AK01's run (which carried a green
baseline). Re-verified scoped before the merge gate.

### How the vacuity controls will be verified, and why the table is not enough

`scripts/mutation-battery.sh` deletes its per-batch xcodebuild logs on a
successful run (`KEEP_WORK=1` is set only on failure paths), so a SURVIVED
verdict would ordinarily leave NO evidence that the suites ran at all — and the
battery scores a NO-VERDICT test as a PASS, so a crash-looping batch prints
SURVIVED with zero observed failures, which is indistinguishable on the results
table from a genuine control.

So the logs are mirrored out while they are being written, and
`verify-survived.sh <batch>` reads the control's own log for four independent
signals: the number of `◇ Test … started` lines, the Swift Testing summary, the
`** TEST SUCCEEDED **` outcome, and the absence of `Restarting after unexpected
exit` / `Killed: 9` / `Test crashed with signal`. It **fails closed** — a
missing log, a missing summary, or an absent outcome all report SUSPECT rather
than OK. Self-tested against a live batch mid-run, where it correctly refused.

Useful number that fell out of the self-test: `FOCUSED_SUITES` is **2,577 tests
in 251 suites**, not the handful this bead added. That is the population every
KILLED verdict above was measured against.

### The verifier itself had the defect it was built to catch

The first version of `verify-survived.sh` located a batch log by searching every
`/private/tmp/playhead-mutation-battery.*` directory for `batch-<N>.log`. That is
wrong, and it is wrong in this repo's standing way — a value that names one thing
read as though it named another.

**Batch numbers are reused across beads, and the old directories survive.** The
battery keeps its work directory whenever it exits on a failure path
(`KEEP_WORK=1`), so this box carries **38** such directories from other agents'
runs on 2026-08-22 and 08-23. Among them:

    batch-1414.log   2026-08-22 17:44   <- MS99's batch number, playhead-2d6i's own run
    batch-1405.log   2026-08-22 17:27   <- MS06's batch number

MS99 is one of the two vacuity controls this bead has to verify. Asked to
verify today's control, the first version would have found a three-day-old log
belonging to a different bead's run of the same-numbered batch and reported OK —
a control whose survival is "confirmed" by evidence from another investigation.

Closed by a freshness floor: the copier mirrors only files newer than the run's
start, preserving mtimes, and the verifier REFUSES any log older than that floor
by name. **Proven by making it fire** rather than by reading a green result: the
Aug-22 `batch-1414.log` was deliberately placed where the verifier would find it,
and it reported FAIL with the date. A rule that has never been observed to fire
is indistinguishable from a rule whose pattern never matches.

(Also worth recording for whoever reads a listing on this box: `ls -1 <dir> |
tail -4` rendered a directory holding sixteen files as `(empty)`. Shell output
here passes through a summarising proxy; `/bin/ls` and `rtk proxy <cmd>` are the
ways around it, and an ABSENCE claim taken from a summarised listing is not
evidence.)

### Could any verdict recorded EARLIER in this session have read a stale log? No — stated plainly.

The freshness floor went in after twelve behavioural verdicts had already been
recorded, so the question has to be answered rather than left inferable.

**No verdict in this session was ever accepted on batch-log evidence at all.**
Every one of the thirteen recorded so far (AK01–AK12, MS01) is evidenced by the
battery's own `observed failures:` block, written into `mutation-run.log` by the
run that produced it, and compared name-for-name against a prediction committed
beforehand. The batch-log checker exists solely for the SURVIVED direction and
has not yet been used to accept anything: its only two invocations were
self-tests, and it REFUSED both — once on a live mid-run batch (no outcome line
yet) and once on the deliberately planted Aug-22 log.

**And the hazard only runs one way, which is why this is fine rather than
merely unaudited.** A KILLED verdict is evidenced by PRESENCE — a named test
that went red — so a stale log cannot manufacture one; at worst it could fail to
corroborate a kill that the run's own failure list already proves. A SURVIVED
verdict is evidenced by ABSENCE, and absence is exactly what a stale or missing
log fabricates. The two vacuity controls are the only verdicts in this series
that depend on the absence direction, and they are the only ones the floor has to
protect. It is in place before either has run.

Filed onward rather than left here: the finding is a `bd comment` on
**playhead-gjlp0**, the open bead for the battery scoring a NO-VERDICT test as a
PASS. Anyone fixing that bead will build this same checker, and they need to know
the work directory is per-INVOCATION rather than per-bead, that `KEEP_WORK=1` on
failure paths has left 38 of them on this box, and that batch numbers repeat.

## The MS re-aims — playhead-2d6i's mutants, moved by this bead's restructuring

| mutant | what it re-introduces | predicted | observed | verdict |
|---|---|---|---|---|
| MS01 | an announced auto-skip records nothing at all | 18 names | **the same 18, set equality** | KILLED |
| MS02 | **RE-AIMED**: the acknowledgement does not remove the receipt, so one skip becomes a card AND a row | PARTITION, ACCEPTED, WALK, SEAM (4) | **the same 4, exactly** | KILLED |
| MS06 | endEpisode stops clearing the receipts | ENDEP (1) | **ENDEP, exactly** | KILLED |
| MS15 | beginEpisode stops clearing them | REPLAY (1) | **REPLAY, exactly** | KILLED |
| MS07 | the receipt records the SPAN START as where the skip fired | POSITION (1) | **POSITION, exactly** | KILLED |
| MS99 | **VACUITY CONTROL** — the attachment-test local renamed | **MUST SURVIVE** | **0 failures, 2577 tests passed, TEST SUCCEEDED** | **SURVIVED** |

**MS02 is the one that needed re-aiming rather than re-anchoring, and the
distinction matters.** Its old body added a receipt write in BOTH arms of
`emitBannerItem`'s attachment branch — which, after this bead, is what the
shipped code does. Run unchanged it would have been an EQUIVALENT MUTANT scored
as a kill: the tests would have gone red for a change that introduced no defect,
and the ledger would have carried a green row for a rail proving nothing. The
direction MS02 has always owned — double delivery, one skip on two surfaces —
did not disappear; it MOVED to the seam, because the receipt is now written
unconditionally and the only thing standing between one skip and two surfaces is
the removal inside `acknowledgeAutoSkippedBannerDelivery`. That is what it
mutates now, and its four observed victims are its four predicted ones.

### CORRECTION: MS02 would NOT have been "scored as a kill". Here is what would actually have happened.

An earlier paragraph in this file says MS02, run unchanged, "would have been an
EQUIVALENT MUTANT scored as a kill". Asked to state what the equivalence WAS so
a reader could check it, the check falsified the claim. The mechanism matters
more than the label, so both are recorded.

**What MS02's old body did.** It inserted a receipt write immediately above
`emitBannerItem`'s attachment guard:

    missedAutoSkipReceiptsByWindowId[adWindow.id] = MissedAutoSkipReceipt(
        item: item,
        playheadTimeAtSkip: currentPlayheadTime,
        occurredAt: Date()
    )
    guard hasAttachedHost else {
        // playhead-2d6i: nobody is listening, and the caller has already

Under playhead-2d6i that was a real defect: the guard's `else` arm was the ONLY
receipt write, so injecting one above it meant a window that got a card also got
a row — one skip, two surfaces.

**Three measured facts about what it would do now.**

1. Its anchor no longer exists. The two-line anchor above matches HEAD **0
   times** — the guard is now `guard hasAttachedHost else { return }` and the
   comment block moved above the write. So the battery would have reported
   `ERROR — anchor did not apply`, exactly as it did for MS99's second anchor
   earlier in this session. That is the loud path, and it is what actually
   protects here.
2. Had the anchor still matched — and it nearly did, because the guard's comment
   block was mine to keep or move — the injected line would have been a SECOND
   write of the same key, `emitBannerItem` having already written it before the
   guard (verified: the write's index precedes the guard's in the shipped body).
3. The two writes differ in exactly one field, `occurredAt: Date()`, and
   **nothing observes it** — no test in the tree asserts on a receipt's
   `occurredAt`, and the list surface deliberately shows the episode span
   instead. So the mutation would have been behaviourally inert.

**So the verdict it would have produced is SURVIVED, not KILLED.** That is a
different failure and a milder one: a survivor is reported as a coverage hole,
which sends a reader to strengthen `THE EXTENDED PROPERTY` — a rail that is
already correct — rather than crediting a rail that is not. Wasted rounds, not a
false green.

**What the episode is actually evidence for**, and it is worth more than the
original claim: a mutant is written against a SHAPE OF CODE, and a bead that
restructures that shape silently invalidates it in one of three ways — the
anchor stops matching (loud), the edit becomes inert (a false SURVIVED), or the
edit becomes the shipped behaviour (a false KILLED). Only the first is
self-announcing. That is why every MS mutant this bead touched was re-read
against the new source rather than re-anchored mechanically, and why MS02 was
given a new body instead of a new anchor.

## Three ways a mutant stops being evidence, and they are NOT the same failure

This branch hit two of them, and an earlier version of this file described the
second as if it were the third. Naming all three is what stops that recurring,
because the three differ in the direction they fail and only one of them is
silent in the direction that looks like success.

**1. THE LOST RAIL — the mutant is killed, but not by the property you think.**
`AK11` re-creates this bead's defect in `observeBanners` and is correctly
KILLED. It cannot, however, prove the delegation canary's *strengthening*,
because it keeps the parameter named `queue` and therefore dies on the very
substring the demonstrated bypass renames (`bannerQueue.enqueue(` contains no
lowercase `queue.enqueue(`). A canary hardened against the bypass would report
green under AK11 having never exercised the new property.
*Verdict shown:* KILLED. *Failure direction:* a rail believed proven that is
not. **Silent, and green.** This is the shape the previous bead's `FD06` v1 had.
*Remedy:* `AK17`, which re-creates the bypass verbatim, rename included.

**2. THE INERT MUTANT — the edit no longer changes behaviour.**
`MS02`'s old body injected a receipt write above the attachment guard. Under
playhead-2d6i that was the whole defect; under this bead the receipt is already
written unconditionally above that guard, so the injection would have been a
second write of the same key differing only in `occurredAt`, which nothing
observes.
*Verdict shown:* SURVIVED. *Failure direction:* a coverage gap reported where
none exists — a reader goes hunting for a missing rail on `THE EXTENDED
PROPERTY`, a rail that is already correct and already killed by five other
mutants. **Loud, but misdirecting.** Wasted rounds, not a false green.
*Remedy:* re-aim, which is what MS02 got — the DIRECTION it owns (one skip, two
surfaces) moved to the seam, so that is what it mutates now.

**3. THE FALSE KILL — the edit becomes the shipped behaviour.**
A mutant whose "defect" is what the code now does turns the suites red for a
change that introduced nothing, and the battery credits the rails that went red.
*Verdict shown:* KILLED. *Failure direction:* a rail credited for catching a
defect that was never injected. **Silent, and it looks exactly like success.**

**Which one MS02 was.** The withdrawn sentence in this file called it (3). It is
(2). The distinction is not pedantry: (3) would mean a rail in this bead is
credited on nothing and the ledger's KILLED column is corrupt; (2) means a
mutant would have wasted a round pointing at a gap that is not there. Only (3)
would invalidate anything already recorded, and it did not happen — verified,
because MS02's old anchor matches HEAD zero times and would have errored before
running at all.

**And a fourth case that is not a failure, because it announces itself.** Anchor
drift: the source moved and the anchor matches 0 or 2+ times, so `patch` refuses
and the battery prints `ERROR — anchor did not apply`. That happened twice in
this session (MS99's second anchor, and MS02's would have) and cost minutes each
time. A mutation battery that can only fail loudly is the cheap case; the work
is in the three above.

## THE TWO VACUITY CONTROLS — both SURVIVED, demonstrated rather than asserted

17 KILLED, 2 SURVIVED, 19/19 recorded. Neither control died, so nothing above
them is void. Their evidence is captured here because a demonstration that
only ever existed in an agent's context is an assertion to everyone who reads
the branch later.

#### AK99 (batch 1432)

```
--- the battery's verdict line ---
AK99   SURVIVED  VACUITY CONTROL — the local the acknowledgement seam binds its receipt to is renamed and nothing else is. MUST SURVIVE
--- observed failures block (must be EMPTY for a control) ---
(none — no test failed under this mutation)

--- host health, read from the batch's OWN xcodebuild log ---
VERIFY: log /Users/dabrams/playhead-gate-artifacts/8cjo/batchlogs//batch-1432.log (4841576 bytes, 2026-08-25 08:54)
VERIFY: tests started   : 2771
VERIFY: swift summary   : Test run with 2577 tests in 251 suites passed
VERIFY: xcodebuild out  : ** TEST SUCCEEDED **
VERIFY: failure lines   : 0
VERIFY: crash markers   : 0
VERIFY: OK
```

#### MS99 (batch 1414)

```
--- the battery's verdict line ---
MS99   SURVIVED  VACUITY CONTROL — the attachment-test local in emitBannerItem is renamed and nothing else is. MUST SURVIVE
--- observed failures block (must be EMPTY for a control) ---
(none — no test failed under this mutation)

--- host health, read from the batch's OWN xcodebuild log ---
VERIFY: log /Users/dabrams/playhead-gate-artifacts/8cjo/batchlogs//batch-1414.log (4828578 bytes, 2026-08-25 08:59)
VERIFY: tests started   : 2772
VERIFY: swift summary   : Test run with 2577 tests in 251 suites passed
VERIFY: xcodebuild out  : ** TEST SUCCEEDED **
VERIFY: failure lines   : 0
VERIFY: crash markers   : 0
VERIFY: OK
```

#### The freshness gate, proven by making it refuse

MS99 is batch **1414**, and a `batch-1414.log` from **playhead-2d6i's own MS99
run three days ago** is still on this box. A checker that searched
`/private/tmp/playhead-mutation-battery.*` would have read that one. Both files
exist right now:

```
today's  (accepted): 2026-08-25 08:59  4828578 bytes
Aug-22's (refused) : 2026-08-22 17:44  4646131 bytes
RUN_FLOOR          : 2026-08-25 07:50
```

And the checker refusing the stale one, run against it deliberately:

```
VERIFY: FAIL — /private/tmp/claude-501/-Users-dabrams-playhead/stalebl//batch-1414.log predates this run (2026-08-22 17:44).
VERIFY: batch numbers are REUSED across beads: a batch-1414.log from 2026-08-22
VERIFY: (playhead-2d6i's own MS99 run) sits in /private/tmp to this day, and
VERIFY: reading it as today's control is the exact substitution this bead is about.
exit=1
```

## The fix round — AK13–AK18, and the one that earned its keep

Run after review's findings landed, one mutant per invocation as before.

| mutant | predicted | observed | verdict |
|---|---|---|---|
| AK13 the delivered-record removal at the write site | REANNOUNCE (1) | **REANNOUNCE, exactly** | KILLED |
| AK14 the retire arm of the forwarding rule | RETIRE (1) | **RETIRE, exactly** | KILLED |
| AK15 the suggest acknowledgement deleted | SUGGEST, ACKAUTO (2) | **ACKAUTO only** | **SURVIVED** → re-aimed → KILLED |
| AK16 a `default:` arm on the tier switch | EXHAUSTIVE (1) | **EXHAUSTIVE, exactly** | KILLED |
| **AK17 the demonstrated bypass, rename included** | **NOCOPY only** | **NOCOPY, exactly** | **KILLED** |
| AK11 re-run against the strengthened canary | DELEGATES, NOCOPY (2) | **the same 2** | KILLED |
| AK18 the suggest ack escapes the guard | SUGGEST, GUARD (2) | **the same 2** | KILLED |
| AK99 control, on the FIXED tree | MUST SURVIVE | 0 failures | **SURVIVED** |

### AK17 settles the question it was written for

`AK11 -> {DELEGATES, NOCOPY}` and `AK17 -> {NOCOPY}`, both exactly as predicted.
The difference between those two victim sets IS the strengthening: AK17 keeps a
`BannerHostDelivery.forward(` call in an else branch, so the POSITIVE check
passes and only the body-scoped forbidden check can see it. Had the canary still
been file-wide on `queue.enqueue(`, AK17 would have SURVIVED — the rename to
`bannerQueue` puts a capital Q in the way of the lowercase substring. It did not
survive. **The strengthening is real, and it is now the thing that would catch
the bypass a reviewer wrote out by hand.**

### AK15 SURVIVED first, and the survivor was right

Its expectation named `A refused SUGGEST item is not acknowledged, so it
survives for the next host`. AK15 DELETES the suggest acknowledgement — and a
test asserting that nothing was acknowledged is satisfied perfectly by there
being no acknowledgement at all. **An absence claim satisfied by a total
absence**, which is this bead's own defect class living on the tier this bead
did not touch. Nothing anywhere asserted the POSITIVE direction.

The remedy was the battery's own instruction — *write the test that rejects it,
do not relax the expectation*. `An ACCEPTED suggest item IS acknowledged` is
that test; without it, a regression that stopped acknowledging accepted
suggestions would have `replayPendingSuggestBanners` hand the same span to every
host that attaches, asking the listener about it again and again, with every
suite green. AK18 is its mirror so the refusal direction is proven too, and it
is deliberately NOT "delete the guard" — that is AK02, which unguards both tiers
and therefore cannot isolate the suggest one.

### Final tally

**25 distinct mutants: 23 KILLED, 2 SURVIVED — and the 2 are the vacuity
controls.** Every kill reddened exactly the tests its prediction named. One
mutant (AK15) survived on its first run, and the coverage hole it exposed was
closed with a new rail rather than by re-aiming the expectation at something
already green.

## The merge-candidate full-plan gate

`scripts/fast-gate.sh`, `PlayheadFastTests`, 2026-08-25.

```
11,806 tests in 1,444 suites · 276.5 s · ** TEST FAILED ** · GATE_EXIT=65
gate-baseline: RED (4 known / 1 NEW) — 3 tests hit a RESOURCE FAILURE (re-run)
host restarts: 0 · NO VERDICT: 0
gate-memory: test host peak open fds 2454 of RLIMIT_NOFILE soft 2560 (95.9 %)
```

**The 1 NEW is not this diff's**, and it is the one named in advance:
`AnalyticsCounterStoreTests` / `The shared store is volatile under XCTest`
(`UserDefaults.standard.data(forKey:) == nil` — process-global state under
XCTest). It has failed on other trees carrying none of this work.

**The 3 RESOURCE denials are not this diff's either**, and the gate says so
itself: all three are `AdWindowIngestAuditTests`, all three are `unable to open
database file`, and the gate's own diagnosis is descriptor exhaustion in the
single test host — playhead-vk68m, which owns the fix. A RESOURCE failure means
the test was never judged, so it is not triageable against any diff.

**Every test this bead adds or changes RAN and PASSED in that run** — checked by
name, including both parameterised tests' three cases each. (The first pass of
that check reported two of eighteen as unclean and was wrong: Swift Testing
prints a parameterised pass as `… with 3 test cases passed`, which a pattern
expecting `" passed` cannot match. The tests were fine; the checker was not —
which is the same reading error this bead is about, in the verification of it.)

**One number worth recording against a review concern.** A reviewer predicted
that ~12 new concurrent `AnalysisStore` instances (≈36 descriptors at the
repo's measured 3.00 fds/store) could tip a host already measured at 2,539 of
2,560. The observed peak on this run is **2,454 — 85 BELOW that earlier
measurement**, so the addition did not move the ceiling. The suite is at 95.9 %
of the soft limit either way, and that is playhead-vk68m's to fix.
