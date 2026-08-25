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
| AK99 | **VACUITY CONTROL** — the seam's receipt binding renamed, nothing else | **MUST SURVIVE, 0 failures** | | |

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
