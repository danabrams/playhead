#!/usr/bin/env python3
"""playhead-voez — make the default gate's verdict about the DIFF, not the count.

WHY THIS EXISTS
---------------
`scripts/fast-gate.sh` exits 65 on a clean checkout, every time, with dozens of
failures nobody introduced. A gate that is red no matter what cannot answer the
only question it exists to answer — *did my change break something?* — so every
bead in flight was hand-diffing its failure set against a baseline nobody had
written down. That is not a hypothetical cost: playhead-aqo9 burned six extra
builds and retracted two conclusions over it, and a real regression
(playhead-ynmk, #313) merged as "looks like the usual flakes".

So: record what is known-broken in a committed file, and make the gate's exit
code a statement about the difference between this run and that file.

    RED (N known / 0 new)   -> exit 0
    RED (N known / 2 NEW)   -> exit non-zero, both named
    a baseline test PASSES  -> exit non-zero, named  (Dan, 2026-07-29: "yes")

THE HARD PART: THE BASELINE SET IS NOT STABLE
---------------------------------------------
An exact set is the right hygiene but a flat exact set does not survive contact
with this machine. Measured full-gate failure counts on unchanged main ranged
33 -> 46 -> 60 -> 72 across one day, and 50 of 62 recorded issues in the
2026-08-01 run were literally `Time limit was exceeded: 60.000 seconds`. The
failing set is not a property of the tree; it is largely a property of how
starved the box was. Membership, not just count, moves.

The design that survives that has three parts.

1. TIERS DERIVED FROM MEASUREMENT, NOT TASTE. Every entry carries how many
   observations it was seen in and how many it failed in. `failed == seen` with
   at least MIN_RUNS_FOR_DETERMINISTIC observations makes it DETERMINISTIC; anything else is
   LOAD-SENSITIVE. Nobody hand-labels a test flaky — the file records counts and
   the tier falls out. The threshold is three observations, not two, because the
   measured run-to-run Jaccard is 0.46 — see MIN_RUNS_FOR_DETERMINISTIC.

2. DAN'S PASS-DIRECTION ARM APPLIES WHERE IT IS SOUND. A DETERMINISTIC member
   that passes fails the gate: that entry claimed to fail every time and did
   not, so the list has rotted and must shrink. A LOAD-SENSITIVE member that
   passes is reported as a removal candidate but does not fail the gate,
   because a single quiet run is not evidence that a starvation flake is fixed.
   Making it fail there would fire on every quiet box, and a gate that cries
   wolf is a gate people learn to bypass — which is the failure mode this bead
   is fixing, not one to re-introduce.

3. THE TOLERANCE IS NOT A HOLE, BECAUSE IDENTITY INCLUDES THE FAILURE KIND.
   This is the load-bearing part. A load-sensitive entry is not "this test may
   fail"; it is "this test may TIME OUT". If a known-timeout test instead fails
   an expectation, its kind is not in the recorded set and it is reported as
   NEW. So the only regression the tolerance can absorb is one that (a) lands
   in a test already named in the file and (b) manifests as a >=60s starvation
   rather than an assertion — i.e. a fresh deadlock inside an already-starving
   test. That residue is named, bounded and written down here; everything else,
   including every regression in every test not in the file, is reported.

Three further arms close the ways a failure could hide without being seen:

  * ABSENT — a baseline member that neither passed nor failed did not run at
    all. A name nobody can reach is not evidence (two of playhead-djl0's
    mutation rails "survived" for exactly this reason), so it fails the gate.
  * INCOMPLETE — a log with no terminal verdict is a fragment, and every test
    after the cut looks like it never ran. Refuse to judge rather than report
    green. The 2026-08-01 log everyone quoted "72 failures" from is such a
    fragment: 930 XCTest cases started, 900 reported, no terminal marker.
  * FICTION — zero failures against a non-empty baseline. Against a measured
    floor of dozens that is categorical, not quiet.

A CRASHED HOST PRODUCES NO VERDICT, AND THAT IS NOT A FAILURE (playhead-tl6l)
----------------------------------------------------------------------------
Everything above reads per-test result lines. A test whose HOST died emits
none: no `failed after`, no `Test Case … failed`, no issue. It is therefore
absent from the run's observed set entirely — matched against the baseline as
neither "known" nor reported as NEW. It falls out of the arithmetic in BOTH
directions, silently, and the run still prints a confident `RED (N known /
0 new)` and exits 0. Same shape as the wedge `scripts/disk_preflight.py` exists
to catch: the failure destroys the evidence of itself.

MEASURED on THREE full-plan runs, 2026-08-12/13. All three carry xcodebuild's
restart marker, and after discounting skips:

    main @ 76b0a09a  11 Swift Testing tests started and reported NOTHING
    bead/mn5e        11                    "                          "
    bead/tl6l        15                    "                          "

The first two are THE SAME ELEVEN — every one a download/cache/streaming test,
the same family xcodebuild's own `Failing tests:` block names. The baseline file
(117 tests over 9 observed runs) had never once recorded any of them. The third
run is those eleven again plus four from other families that had PASSED in both
earlier runs, and it is the reason the record is a union with counts rather than
a replaced set — see "A UNION WITH OBSERVATION COUNTS" below.

THAT NUMBER HAS BEEN WRONG THREE TIMES, each for a different reason and every
one the same defect class as the bead itself — a value that names one thing
read as though it named another. The history is kept because it is the best
evidence there is about how this census fails:

  * 19 / 18 — the bead's own filing, from grepping the `Failing tests:` block's
    `Suite.function()` spelling against a console that prints Swift Testing
    DISPLAY NAMES. That grep can only ever return zero. See below.
  * 33 / 15 — right method, but every outcome pattern required the literal word
    `seconds`, and xcodebuild splices app output MID-TOKEN (`passed after
    100.732 secon` + a log line). Four tests that PASSED were counted as
    casualties. Fixed at R1 review: the patterns match on the VERB now.
  * 30 / 14 — R1's own re-derivation, by two routes that agreed with each
    other and were both wrong the same way. The splice does not stop at the
    duration: it lands inside the NAME (`✔ Test "a b` + `yte-exact span…`),
    inside the verb (`" pa` + `ssed after 107.082 seconds.`), and once inside
    the word `Test` itself (`✔ Tes` + `t "watermark within…`). Eighteen such
    lines on main and three on mn5e, EVERY ONE a passing test. No pattern over
    `Test "` can see them, because the intrusion carries its own newline and
    splits one logical line into two physical ones. Fixed at R2 review by
    REJOINING before parsing — see `rejoin_spliced_lines`.

Two lessons worth more than the number. First, a wrong census here is not
noise: playhead-buvn arms the gate on it, and 19 of main's 30 were an artefact
of how chatty the app happened to be on that run — arming on that would have
been arming on log interleaving. Second, the direction that has never yet been
hit is the dangerous one: 88 fail lines against 10,910 pass lines is the only
reason it was passes that got severed, and a severed FAIL is a LOST FAILURE
that reads as a crash casualty.

So this module now tracks a third outcome and a fourth verdict category:

  * SKIPPED is parsed (`➜ Test "x" skipped:` and XCTest's `skipped (0.0s)`),
    because "started and said nothing" is only meaningful once a deliberate
    skip has been subtracted from it. Without that the no-verdict set on those
    same two logs reads 60 on main and 44 on mn5e, and every one of the 30
    subtracted on each is an XCTest PerfGate skip — a number that means one
    thing and is read as another, this repo's standing defect class. (Swift
    Testing's own
    skips contribute nothing to that difference: a trait-disabled test never
    emits a `started` line, so it can never be in `started - ran`. The ➜
    pattern is defensive, not load-bearing — measured, R1.)
  * NO VERDICT — started, then neither passed, failed nor skipped. Exact,
    console-only, no name mapping required. This is the census.
  * HOST RESTART — xcodebuild's own `Restarting after unexpected exit, crash,
    or test timeout` line, quoted as corroborating evidence.

WHY THE `Failing tests:` BLOCK IS A LEAD AND NOT A CENSUS
---------------------------------------------------------
The obvious fix — diff xcodebuild's `Failing tests:` summary against the tests
that reported — cannot be done soundly from a log, and the reason is worth
writing down because it is what the bead's own evidence tripped over.

The summary prints `SuiteType.function()`. Swift Testing's console prints the
test's DISPLAY NAME, `@Test("A completion delivered to a NEW manager instance
still carries the show")`. The two spellings share nothing. Grepping a summary
entry against the console therefore returns zero for a test that reported
perfectly well — which is exactly how playhead-tl6l came to be filed claiming
19 invisible failures. Re-measured through the source-level mapping: of the 15
distinct summary names in the mn5e run, ALL 15 had reported and were already
counted; of the 14 on main, 13 had. The real number of block entries invisible
to this module was 0 and 1, not 19 and 18. The blindness is real, the
population named in the bead was not.

And the residue cuts both ways: `Suite.cancelReapsAttribution()` (reported
under a display name) and `Suite.failedSuggestNoRestoresLatestBufferedRevision()`
(the one genuine casualty on main, which never emitted a line at all) are
INDISTINGUISHABLE from the log. Resolving them would mean parsing
`PlayheadTests/**.swift` for `@Test("…")` attributes, which trades a sound
log-only tool for a source-coupled heuristic whose every miss is a false crash
alarm. Not worth it: the NO VERDICT census above already covers every casualty
that got as far as starting.

So the block is parsed, reported with an exact entries-vs-distinct count, and
labelled a LEAD. It earns its place three ways: it names a casualty that never
started (invisible to the started-set); it makes the duplicate entries legible
(xcodebuild repeats a name it retried — 4 of 19 on mn5e); and it lets an ABSENT
baseline member be reported with the RIGHT CAUSE. Before this, a crashed
baseline member read `(renamed, deleted or newly skipped)`, sending the reader
to look for a rename that never happened.

WHAT NO VERDICT DOES TO THE EXIT CODE, AND WHY (playhead-buvn)
--------------------------------------------------------------
It is ARMED — on the DIFF against a record, exactly like every other category
in this file, and not on the count.

tl6l shipped it reportable-but-not-fatal, and the argument was sound as far as
it went: the condition fires on main today, on a pre-existing crash owned by
playhead-rouw, so a flat arm would make every full-plan gate on this box exit
65 for a reason nobody in the middle of an unrelated bead can fix — and
CLAUDE.md is explicit that "one red rule and everyone learns to route around
the gate" is strictly worse than having no linter. But that is an argument
against arming it FLAT, not against arming it. Every mitigation tl6l shipped
addresses a HUMAN READER, and none of them is an exit code, so the regression
class it could not catch was a change that CRASHES THE TEST HOST: the tests it
kills are healthy ones in nobody's failure baseline, so `new_failures` is empty
(they emitted no failure line) and `absent` is empty (that arm covers only
already-recorded tests) — and the run exits 0. The crash destroys the evidence
of itself, which is the exact shape the bead was filed against.

So the record is `no_verdict` in the per-plan baseline file, and:

  * a name that lost its verdict and is NOT in the record is a NEW CASUALTY. It
    is NAMED on every run, and it FAILS the gate once the record is ARMED. That
    is the arm, and it is the only thing in this module that can see a fresh
    crash;
  * a name that IS in the record is quiet — the pre-existing crash stays
    somebody else's bead;
  * a recorded LOAD-SENSITIVE name that reported an outcome is REPORTED AGAIN,
    good news, not fatal. One quiet run is not evidence a crash is fixed, which
    is the same reason a load-sensitive failure passing is a candidate and not
    a verdict;
  * a recorded DETERMINISTIC name that reported an outcome FAILS the gate. Its
    entry claims it loses its verdict in every observation and it did not, so
    the licence has rotted — Dan's pass-direction call, applied where it is
    sound. See below for why this one is not optional;
  * the COUNT alone is not fatal, and neither is a bare HOST RESTART. A restart
    that costs nobody a verdict means every test was re-run and no information
    was lost; it is reported, it forecloses GREEN, and it stops there.

A UNION WITH OBSERVATION COUNTS, NOT A REPLACED SET (playhead-tl6l R4)
---------------------------------------------------------------------
R2 recorded a SET and REPLACED it on each accept. The argument was measured and
it was wrong, and the way it was wrong is worth more than the conclusion: two
full-plan runs on DIFFERENT trees lost THE SAME ELEVEN TESTS — Jaccard 1.00,
against 0.46 for the failure set on IDENTICAL code — so a union looked like
machinery for churn there was none of.

A THIRD full-plan run, on the armed code, lost FIFTEEN. The same eleven for the
third time, plus four that had PASSED in both earlier runs, and nothing has ever
recovered. Jaccard 1.00 -> 0.73. Two observations could not have shown it, which
is the whole of the lesson: this population's churn is a property of how starved
the box was that night, exactly like the failure set's, and one quiet pair of
runs is not a measurement of it.

Under a replaced set, an armed gate on a night like that reports four NEW
casualties and exits 65 — red for a reason its reader cannot fix, which is the
hazard R2's own exit-code argument was constructed to avoid, and which CLAUDE.md
calls strictly worse than having no linter. So the census is recorded the way
`tests` is: a union, each entry carrying `seen_runs`/`lost_runs`, with the tier
falling out of the counts. Accept the three runs in order and the eleven stand
at 3/3, DETERMINISTIC; the four stand at 1/1, LOAD-SENSITIVE. Nobody labels
either.

READ 1/1 AS "ONE OBSERVATION", NOT "ONE OF THREE". A name accrues observations
only from the accept that first RECORDS it — the two earlier runs reached those
four and watched them pass, but there was no entry to credit. `tests` behaves
the same way and for the same reason. The distinction matters here because the
across-run measurement (lost in one of three runs) and the recorded entry (1/1)
are different quantities that both read "one", and this file's whole history is
values that name one thing being read as though they named another.

WHAT ARMS, AND WHY EACH WAITS FOR WHAT IT WAITS FOR:

  * A NEW CASUALTY is fatal only once the census has
    MIN_RUNS_FOR_DETERMINISTIC observations. It is NAMED from the first, because
    a newly-observed casualty is indistinguishable from a regression and
    absorbing it silently is the hole this module exists to close. What three
    observations buy is the right to make it fatal — and the reason the census
    gets that grace where a NEW FAILURE does not is the remedy, not the
    evidence: a new failure names a test that failed and can be triaged against
    the diff, while a test with no verdict was never judged at all and the only
    honest response is to run the whole plan again.
  * A DETERMINISTIC entry REPORTING AGAIN is fatal, and this is the price of
    the union rather than a bonus. R2's one real objection to unioning stands
    otherwise: a union can only grow, so a crash that is genuinely fixed would
    stay recorded forever and the gate would never speak about those names
    again. The pass-direction arm is what makes a fixed crash LOUD, on the
    first run that fixes it, so the record can shrink. It cannot fire before
    three observations by construction, and it fires only on POSITIVE evidence
    — the test started and reported. A recorded name that never started at all
    is a rename, a deletion or a host that died earlier, three things
    indistinguishable from a log, so it is reported and never armed.

The record is still a set of NAMES rather than a count, which is the half of
R2's argument the third run does not touch: a count cannot see eleven tests
dying while eleven different ones recover.

WHAT AN ACCEPT DOES TO IT: a name that lost its verdict again is credited
`seen + 1, lost + 1`; a name that started and REPORTED is credited `seen + 1`
alone, which is the only thing that demotes an entry; a name that never started
is PRUNED on a healthy run (renamed, deleted, newly skipped) and CARRIED FORWARD
untouched on a crashed one, because pruning there is how a crash would shrink
the record from inside the one command meant to maintain it.

WHAT THE SET CANNOT SEE, named rather than glossed:

  * A TEST THAT NEVER STARTED. The census is `started - ran - skipped`, so a
    host that dies before a test's start line leaves nothing to subtract. That
    casualty is invisible here and shows up, if at all, as an unmatched
    `Failing tests:` entry — a LEAD, below, and deliberately not armed.
  * IDENTITY IS THE DISPLAY NAME, so two same-named tests in different suites
    share one key (0.57 % of names, measured). A casualty whose twin reported
    is not in the set.
  * A RENAME reads as one recovery plus one new casualty, i.e. a false RED. The
    remedy is the one a rename already demands of the failure baseline —
    `--accept-baseline` — and it is the price of precision, paid knowingly.
  * HOW MUCH WAS LOST. Eleven names may be eleven tests or the visible edge of
    a suite that died before any of it started. The set says which verdicts are
    missing, never how much of the plan went unjudged.

THE `.xcresult` IS NOT A BETTER SOURCE. IT IS A WORSE ONE (R3, 2026-08-13)
--------------------------------------------------------------------------
Five censuses in a row were derived from this text log, so the sixth was
derived from the structured result bundle instead — a database, immune to
stdout interleaving by construction. It CONFIRMS 11 and 11, the same eleven,
on both runs. It also shows why the bundle must not replace the console.

  * The bundle's TEST TREE has no unjudged nodes at all: every node carries
    Passed / Failed / Skipped. "Present but unjudged" is EMPTY there, so a
    casualty is ABSENT-FROM-BUNDLE, and absence needs a roster to be visible.
  * The roster is the bundle's ACTION LOG, which carries a `Run test case`
    section per test titled `Suite/function()` — 11,893 on main, and a strict
    superset of the 10,845 in the tree (0 in the tree are missing a section).
  * 1,048 of those sections have `duration: 0` and NO tree node: their results
    were never committed to the database. 1,035 of the 1,048 demonstrably RAN
    AND PASSED — `high confidence FM result shows brand name` is in the console
    as `passed after 195.586 seconds` — because stdout was flushed and the
    result stream was not. On mn5e it is 1,121 lost and 1,108 with a console
    verdict.
  * Exactly 13 of the 1,048 lack a console verdict, and 2 of those are an
    artefact of resolving a `@Test` whose display name is a Swift multi-line
    string literal. The remaining ELEVEN are the census, on both runs.

So the bundle is a sound INDEPENDENT ROSTER and a bad outcome source: its own
casualty population is ~95x larger than the truth. Two things it gives that the
console cannot, both used by playhead-rouw: the structured `Suite/function()`
identity (the eleven are DownloadManagerCacheTests 5,
DownloadManagerImmutableArtifactTests 3, BackgroundDownloadCompletionTests 2,
StreamingDownloadOwnershipTests 1 — identical on both runs), and the fact that
the crash banner carries NO message in either the console or the bundle.

Two corroborations fell out of it. The skip parsing is exact — the bundle
records 41 skips on main and 42 on mn5e, and this module counts 41 and 42. And
the display-name collision cost is 64 colliding names over 141 ids, 0.59 % of
10,833, with NONE of the eleven sharing a name with a test that reported: the
collision limit above is real but does not touch this census.

UNRECORDED IS NOT ZERO, AND PROVISIONAL IS NEITHER. A baseline with no
`no_verdict` key has never recorded the population: the arm is INERT and the
verdict says so out loud. A key present with 1-2 observations is PROVISIONAL: a
real measurement, too few observations to bound a population this
load-sensitive, so a casualty nobody recorded is named and is not fatal. A key
present with MIN_RUNS_FOR_DETERMINISTIC or more is ARMED — including a recorded
census that is EMPTY, which is a positive claim that no test should lose a
verdict. This is what lets the arming land without turning main red today, and
the three are deliberately not spelled the same way, because reading an absence
as a measurement is this repo's standing defect class and this key has now been
its site twice.

The other three tl6l mitigations are unchanged and still carry the run:

  * the headline carries the count, so the reassuring `RED (N known / 0 new)`
    can never stand alone again — it reads `RED (85 known / 0 new) — 11 tests
    got NO VERDICT (crashed host)`;
  * GREEN is unreachable while the count is non-zero, on the same principle
    that already forbids GREEN for a run that executed nothing;
  * a baseline member with no verdict still fails the gate, because it is
    ABSENT — unchanged policy, with the crash named as the cause;
  * `accept` CARRIES FORWARD a baseline entry that got no verdict instead of
    pruning it as a rename, so a crash cannot shrink the file from inside the
    one command meant to maintain it.

THE FILE CONVERGES; IT DOES NOT ARRIVE COMPLETE
-----------------------------------------------
The recorded set is the UNION of what has been observed, and with a measured
run-to-run Jaccard of 0.46 each new run surfaces flakes the earlier ones missed.
Capture-recapture on the first two full runs (32 and 28 failures, 19 shared)
puts the true population near 47; 41 are recorded after two observations. So the
next accept or two will legitimately add a handful of names.

That is not a defect to engineer around. A newly-observed flake is
indistinguishable from a regression until it has been seen at least once, and
absorbing unrecognised names on the theory that they are probably flakes is the
exact hole this module exists to close. Report, record, move on.

KNOWN RESIDUE, WRITTEN DOWN RATHER THAN GLOSSED
-----------------------------------------------
Swift Testing's console line prints the test's display name and no suite, so two
same-named tests in different suites share one key. Measured on the 2026-08-01
full run: 56 of 9,819 names, 0.57%. Identity is resolved toward FAILED — a
colliding pass never erases a failure — so the dangerous direction is closed,
and the residue is that a regression landing in the passing twin of a colliding
pair while the other twin still fails would read as known. Adding the source
file to the key would separate them, but only failures carry a source (it comes
from the issue line), so a baseline key built that way could never be matched
against a pass and the pass-direction arm would stop working. The collision is
the cheaper of the two costs.

XCTest has no such problem: its key is the fully qualified
`Target.Suite/testMethod`.

BOTH FRAMEWORKS, AND WHY THE HEURISTIC INVERTS
----------------------------------------------
Swift Testing prints `✘ Test "name" failed after 123.4 seconds`; XCTest prints
`Test Case '-[Suite testFoo]' failed (0.025 seconds)`. Triage that greps only
the first is how playhead-ynmk merged unnoticed, so both are parsed here.

They behave oppositely and duration is the trap: a slow Swift Testing failure is
usually starvation, while a slow XCTest failure is still an assertion. This
module therefore never classifies on duration at all. Swift Testing kind comes
from the issue text (`Time limit was exceeded` -> timeout, anything else ->
assertion), which is the direct evidence rather than a proxy; XCTest failures
are always assertions, because XCTest has no time-limit issue to report.

WHAT AN ACCEPT MUST SAY OUT LOUD
--------------------------------
`accept` reported membership and nothing else until playhead-26od R5, and both
halves of that omission had already done damage on one branch.

  * The KIND of each added entry was never printed, so the third accept — 28
    entries, three of them assertion-ONLY and a fourth mixed — was justified in
    its commit message as "all timeouts". Under this module's own identity rule
    a load-sensitive entry means MAY TIME OUT, so that is a different claim, and
    it was written from the count because the count was all the tool showed.
  * TIER PROMOTIONS were never printed, so the same accept crossed fifteen
    entries into `deterministic` — arming the pass-direction check, after which
    each of them PASSING hard-fails the gate — and said nothing. A hard failure
    armed silently is the exact species of quiet this file exists to remove.

Both are now printed: a kind census plus a per-entry kind on every `+` line, and
a loud `ARMED:` block naming every promotion with its `failed/seen` count. The
POLICY is untouched — what is deterministic, and that its passing is fatal, is
Dan's call and lives at `MIN_RUNS_FOR_DETERMINISTIC`. Only the reporting changed.

And the SAME OMISSION EXISTED IN THE OPPOSITE DIRECTION, on both records, which
is worth stating as one rule rather than two fixes (playhead-o89d R1 and R2). A
promotion arms a hard failure; a DEMOTION revokes one, which is the gate getting
LOOSER, and it was the quietest line in the output on the `tests` side (four bare
words) and had no line at all of its own on the census side (an entry coming back
was spelled `~= reported again`, identically to the load-sensitive case where
coming back is good news and costs nothing). Both now print a banner naming the
tier being LEFT, the event, and the consequence.

FOUR TIER BANNERS, FOUR SPELLINGS, AND THE RULE IS THE PREFIX (playhead-o89d R3).
R2 gave the two DEMOTIONS distinct spellings and wrote the rule down as "neither
side can be read as the other" — while leaving the two PROMOTIONS spelled
identically, both `ARMED:`, with opposite consequences. Measured at R3: a mutant
that respells the census promotion as the `tests` one VERBATIM, so the operator
is told "Each of these PASSING now fails the gate" about a crashed-host name,
SURVIVED the whole suite; so did one that changed the census banner's own
`REPORTING AGAIN` to `PASSES`. The rule now holds in both directions and is
carried by rails rather than by prose:

    ARMED:               a recorded FAILURE became deterministic; its PASSING is fatal
    DISARMED:            …and stopped being; that licence is revoked
    CENSUS ARMED:        a recorded CASUALTY became deterministic; its REPORTING is fatal
    CENSUS DISARMED:     …and stopped being; that licence is revoked

`CENSUS RECORD ARMED:` is deliberately none of the four. Its subject is the
record rather than an entry — the census reached MIN_RUNS_FOR_DETERMINISTIC
observations, so a casualty NOT in the record is now fatal — and while it was
spelled `CENSUS ARMED:` it read as the counterpart of `CENSUS DISARMED:`, which
it is not.

USAGE
-----
    gate_baseline.py check  --log RUN.log --baseline scripts/gate-baseline.json
    gate_baseline.py accept --log RUN.log --baseline scripts/gate-baseline.json

`check` is what `scripts/fast-gate.sh` runs; `accept` is what
`scripts/fast-gate.sh --accept-baseline` runs. Exit codes: 0 ok, 1 regression,
2 could not evaluate.
"""

import argparse
import json
import pathlib
import re
import sys

EXIT_OK = 0
EXIT_REGRESSION = 1
EXIT_CANNOT_EVALUATE = 2

KIND_TIMEOUT = "timeout"
KIND_ASSERTION = "assertion"
KIND_UNKNOWN = "unknown"

TIER_DETERMINISTIC = "deterministic"
TIER_LOAD_SENSITIVE = "load-sensitive"

# How many observations an entry must fail in a row before its PASSING is
# allowed to fail the gate.
#
# MEASURED, not guessed. Two full runs on identical code, same quiet box,
# nothing else running: 32 failures and 28 failures, 19 in common, union 41 —
# a Jaccard of 0.46. Under that much churn "failed twice" is weak evidence of
# "fails every time", and every entry wrongly promoted becomes a NOW PASSES
# gate failure on some later quiet run.
#
# The asymmetry decides it. A false pass-direction alarm costs a gate run, a
# refresh commit, and a little more of the trust this bead exists to restore —
# and a gate people stop believing is the exact thing being fixed. A missed one
# costs a stale name that is still printed as a removal candidate every run.
# So: be slow to promote, and let the file earn it.
MIN_RUNS_FOR_DETERMINISTIC = 3

# How many names to print per category before collapsing to a count. A verdict
# nobody reads is a verdict nobody acts on.
_MAX_LISTED = 10

FRAMEWORK_SWIFT_TESTING = "swift-testing"
FRAMEWORK_XCTEST = "xctest"


class CannotEvaluate(Exception):
    """The run cannot be judged — refuse rather than guess in either direction."""


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------
#
# Every pattern below SEARCHES rather than anchors. Real xcodebuild logs carry
# \r-overwritten prefixes ("X◇ Test", "​✘ Test" with a zero-width space), and an
# anchored `^\W*` misses the ones whose junk happens to be a word character —
# a silently dropped failure, in the direction that reads as success.

# THE TRAILING ` seconds` IS NOT REQUIRED, AND THAT IS LOAD-BEARING (playhead-tl6l
# R1 review). The same interleaving the comment above describes also truncates a
# line MID-TOKEN: three of main's 10,910 pass lines end `passed after 100.732
# secon` with an app log line spliced straight onto them. Requiring the word
# turned three tests that DEMONSTRABLY PASSED into crashed-host casualties — the
# census read 33 where the truth was 30, and 15 where it was 14. A verdict line
# that loses its duration is still a verdict; the duration is decoration and is
# captured only when it survived intact. Fail and XCTest are widened with it
# because the exposure is proportional to line count, not to kind — 88 fail lines
# against 10,910 pass lines is why only the passes were hit here, and a LOST
# FAILURE is the far worse direction.
_ST_FAIL_NAMED = re.compile(r'✘ Test "(.+?)" (?:with \d+ test cases? )?failed after(?: ([\d.]+) seconds)?')
_ST_FAIL_FUNC = re.compile(r'✘ Test ([A-Za-z_][A-Za-z0-9_]*\(\)) (?:with \d+ test cases? )?failed after(?: ([\d.]+) seconds)?')
# Greedy `.*` before ` at <file>.swift:` so parameterised runs — which splice
# "with 2 arguments depth → 8, mix → preAnalysis" in between — resolve to the
# LAST such marker, which is the source location rather than an argument value.
_ST_ISSUE_NAMED = re.compile(r'✘ Test "(.+?)" recorded an issue(?P<mid>.*?)(?: at ([A-Za-z0-9_+]+\.swift):(\d+):(\d+))?: (.*)$')
_ST_ISSUE_FUNC = re.compile(r'✘ Test ([A-Za-z_][A-Za-z0-9_]*\(\)) recorded an issue(?P<mid>.*?)(?: at ([A-Za-z0-9_+]+\.swift):(\d+):(\d+))?: (.*)$')
_ST_PASS_NAMED = re.compile(r'✔ Test "(.+?)" (?:with \d+ test cases? )?passed after')
_ST_PASS_FUNC = re.compile(r'✔ Test ([A-Za-z_][A-Za-z0-9_]*\(\)) (?:with \d+ test cases? )?passed after')
_ST_START_NAMED = re.compile(r'◇ Test "(.+?)" started')
_ST_START_FUNC = re.compile(r'◇ Test ([A-Za-z_][A-Za-z0-9_]*\(\)) started')
# A DELIBERATE skip is a third outcome, not silence. The 30 XCTest PerfGate skips
# are what the census actually depends on subtracting — without them it reads 44
# on mn5e and 60 on main where the truth is 14 and 30. These two ➜ patterns are
# DEFENSIVE rather than load-bearing, and R1 review measured the difference at
# exactly zero: Swift Testing's ~11 skips are trait-disabled, and a
# trait-disabled test never emits `◇ Test "x" started`, so it cannot be in
# `started - ran` to begin with. Kept because a skip spelled at runtime (a thrown
# skip after the start line) would be, and that is one Swift-Testing release away.
_ST_SKIP_NAMED = re.compile(r'➜ Test "(.+?)" skipped')
_ST_SKIP_FUNC = re.compile(r'➜ Test ([A-Za-z_][A-Za-z0-9_]*\(\)) skipped')

_XC_RESULT = re.compile(
    r"Test Case '-\[([A-Za-z0-9_.]+) ([A-Za-z0-9_:]+)\]' (failed|passed|skipped)"
    r"(?: \(([\d.]+) seconds\))?"
)
_XC_START = re.compile(r"Test Case '-\[([A-Za-z0-9_.]+) ([A-Za-z0-9_:]+)\]' started")

_TERMINAL = re.compile(r"\*\* TEST (FAILED|SUCCEEDED) \*\*|Test run with \d+ tests? in \d+ suites? (?:failed|passed) after")

_WEDGED_SIM = "fast-gate: wedged simulator"

# THE SPLICE, AND WHY IT NEEDS A REJOIN RATHER THAN A WIDER PATTERN (R2 review).
#
# xcodebuild interleaves the app's stdout into the runner's, and it does it
# mid-write INCLUDING THE APP LINE'S OWN NEWLINE. So one logical verdict line
# becomes TWO physical lines, split at an arbitrary byte:
#
#     ✔ Test "flag OFF is byte-identical<TS> Playhead[…] [Capabilities] fm.…
#      for both the exempt and the guarded shape" passed after 96.203 seconds.
#
# Neither half is a verdict on its own. R1 review widened the outcome patterns
# so a line cut inside the trailing ` seconds` still reads as a verdict, which
# fixes the case where the cut lands AFTER the verb — three lines on main, one
# on mn5e. It cannot fix the far commoner case where the cut lands inside the
# NAME or inside the verb itself, and those are invisible to any pattern:
#
#     ✔ Test "a b<TS> …            + `yte-exact span outside the 90s window…`
#     ✔ Test "fast transcript chunks override stale transcript watermark" pa<TS>
#     ✔ Tes<TS> …                  + `t "watermark within one shard past the …`
#
# The last one severs the word `Test`. No regex over `Test "` can ever see it.
#
# MEASURED on the two preserved 2026-08-12 full-plan runs: 18 such lines on main
# and 3 on mn5e, EVERY ONE a test that passed. Scored as silence they became
# crashed-host casualties, which is how the census read 30 and 14 where the
# truth is 11 and 11 — the same defect class as the two corrections before it,
# a value that names one thing read as though it named another.
#
# Undoing the splice recovers the original line BYTE-FOR-BYTE, so the name is
# exact rather than matched by prefix. Doing it here rather than in each pattern
# fixes start, pass, fail, issue, skip and XCTest at once — and the direction
# that matters most is the one these two logs happened not to contain: a severed
# FAIL line is a LOST FAILURE, and 88 fail lines against 10,910 pass lines is the
# only reason it was the passes that got hit.
_APP_LOG_INTRUSION = re.compile(
    r"\d{4}-\d\d-\d\d \d\d:\d\d:\d\d\.\d{6}[-+]\d{4} \w+\[\d+:"
)
# Cheap pre-filter: a head worth trying to repair carries a Swift Testing marker
# glyph or the start of XCTest's own line. Anything else is app output that
# merely happens to contain a timestamp, and must be left exactly as it is.
_REPAIRABLE_HEAD = re.compile(r"[◇✔✘➜]|Test Case '-\[")


def _parses_as_a_test_line(text):
    """Does this text carry a per-test event any of the patterns below can read?"""
    return bool(
        _ST_ISSUE_NAMED.search(text) or _ST_ISSUE_FUNC.search(text)
        or _ST_FAIL_NAMED.search(text) or _ST_FAIL_FUNC.search(text)
        or _ST_PASS_NAMED.search(text) or _ST_PASS_FUNC.search(text)
        or _ST_SKIP_NAMED.search(text) or _ST_SKIP_FUNC.search(text)
        or _ST_START_NAMED.search(text) or _ST_START_FUNC.search(text)
        or _XC_RESULT.search(text) or _XC_START.search(text)
    )


def rejoin_spliced_lines(lines):
    """Undo the app-log intrusion described above. Returns the repaired lines.

    Conservative by construction — three conditions must all hold, and the
    middle one is what makes the repair unable to invent a verdict:

      1. the line carries an app-log timestamp at a position that is NOT the
         start of the line (a line that merely IS app output is untouched);
      2. the head — the bytes before that timestamp — carries a test marker and
         does NOT already read as a test line. A verdict that survived intact is
         never rewritten;
      3. the NEXT line does NOT read as a test line on its own. A record that
         stands up by itself is never swallowed into somebody else's;
      4. head + that next line does read as a test line.

    Only then are the two lines replaced by the reconstruction plus the
    displaced app output, so nothing is dropped and nothing is counted twice.

    CONDITION 3 IS NOT DECORATION — it was added because a rail caught the
    repair without it fabricating a test. Two consecutive severed lines for the
    same test glued into `✘ Test "victim" recorded an iss✘ Test "victim" failed
    after 0.03 secon…`, which the fail pattern reads with the name
    `victim" recorded an iss✘ Test "victim`: one real failure turned into one
    phantom under a name nobody can ever reconcile. Together with condition 2 it
    also forces the match to SPAN the join — it cannot live wholly in either
    half — which is what makes a reconstruction a reconstruction.

    WHAT IT CANNOT SEE, named rather than glossed: a line cut twice (the tail
    displaced past the following line); a cut whose tail is the very last line
    of a truncated log; a cut landing inside the app-log timestamp itself; and,
    by condition 3, a cut whose tail happens to be a whole verdict on its own.
    All four leave a head that stays unparseable, so the failure direction is a
    casualty that is REPORTED — never a verdict invented.
    """
    out = []
    index = 0
    while index < len(lines):
        line = lines[index]
        match = _APP_LOG_INTRUSION.search(line)
        if match and match.start() > 0 and index + 1 < len(lines):
            head = line[:match.start()]
            following = lines[index + 1]
            if (_REPAIRABLE_HEAD.search(head)
                    and not _parses_as_a_test_line(head)
                    and not _parses_as_a_test_line(following)
                    and _parses_as_a_test_line(head + following)):
                out.append(head + following)
                out.append(line[match.start():])
                index += 2
                continue
        out.append(line)
        index += 1
    return out

_TIME_LIMIT = "Time limit was exceeded"

# playhead-tl6l. xcodebuild's own words when the test host died and it started a
# new one. Unambiguous, needs no name mapping, and present in both full-plan runs
# measured on 2026-08-12 — while the verdict printed above it said nothing.
_HOST_RESTART = re.compile(
    r"Restarting after unexpected exit, crash, or test timeout"
)

# The summary block xcodebuild prints after the terminal marker:
#
#     Failing tests:
#     \tDownloadShowAttributionTests.attributionSurvivesProcessRestart()
#     \tDownloadShowAttributionTests.attributionSurvivesProcessRestart()
#
# A repeated name is a RETRY, not two tests. Entries and distinct names are both
# reported, because a count that silently means one when read as the other is
# this repo's standing defect class.
_FAILING_TESTS_HEADER = re.compile(r"^\s*Failing tests:\s*$")
_BLOCK_ENTRY = re.compile(r"^[ \t]+(\S.*?)\s*$")
_BLOCK_NAME = re.compile(r"^([A-Za-z_][A-Za-z0-9_.]*)\.([A-Za-z_][A-Za-z0-9_]*)(\(\))?$")


def st_key(name):
    return FRAMEWORK_SWIFT_TESTING + "::" + name


def xc_key(suite, method):
    return FRAMEWORK_XCTEST + "::" + suite + "/" + method


class Failure(object):
    __slots__ = ("key", "framework", "name", "kinds", "seconds", "source")

    def __init__(self, key, framework, name):
        self.key = key
        self.framework = framework
        self.name = name
        self.kinds = set()
        self.seconds = None
        self.source = None

    def __repr__(self):  # pragma: no cover - debugging aid
        return "Failure(%r, kinds=%r, %ss)" % (self.key, sorted(self.kinds), self.seconds)


class RunResult(object):
    def __init__(self):
        self.failures = {}
        self.passed = set()
        self.skipped = set()
        self.started = set()
        self.complete = False
        # playhead-tl6l
        self.host_restarts = 0
        self.restart_evidence = None
        self.blamed_entries = []   # `Failing tests:` lines, duplicates INTACT

    @property
    def ran(self):
        """Keys with a definite outcome. Started-but-silent is NOT an outcome.

        A SKIP is deliberately not in here. It is an outcome for the purpose of
        "did the host die", and no outcome at all for the purpose of "is this
        baseline entry still failing" — the ABSENT arm must keep firing on a
        newly-skipped member, which is what makes PerfGate-ing a family visible
        rather than a quiet loss of coverage.
        """
        return set(self.failures) | self.passed

    @property
    def blamed(self):
        """The `Failing tests:` names, de-duplicated, first-seen order kept."""
        return list(dict.fromkeys(self.blamed_entries))

    @property
    def no_verdict(self):
        """Started, then said nothing at all — the crashed-host casualties.

        Exact and console-only: both the start line and the outcome line carry
        the same identity, so this needs no mapping between xcodebuild's
        `Suite.function()` spelling and Swift Testing's display names.
        """
        return self.started - self.ran - self.skipped


def last_attempt(text):
    """fast-gate retries once on a wedged simulator and both attempts land in one
    log. Attempt 1's casualties — tests that were mid-flight when the sim died —
    would otherwise union with attempt 2 and manufacture failures out of an
    infrastructure artefact. Keep only what follows the last retry banner."""
    cut = 0
    lines = text.splitlines(True)
    for i, line in enumerate(lines):
        if _WEDGED_SIM in line:
            cut = i + 1
    return "".join(lines[cut:])


def _kind_of_issue(message):
    return KIND_TIMEOUT if _TIME_LIMIT in message else KIND_ASSERTION


def parse_run(text):
    """Parse one xcodebuild gate log into a RunResult."""
    run = RunResult()
    failures = run.failures

    def failure_for(key, framework, name):
        if key not in failures:
            failures[key] = Failure(key, framework, name)
        return failures[key]

    in_block = False
    # Repair the splice BEFORE anything reads a line. Every pattern below is
    # line-oriented, so a verdict cut in half is silence to all of them at once.
    for line in rejoin_spliced_lines(last_attempt(text).splitlines()):
        if not run.complete and _TERMINAL.search(line):
            run.complete = True

        # --- xcodebuild's own summary block (playhead-tl6l) -----------------
        # Read before anything else: its entries are indented names, and a
        # `Suite.func()` line must never be mistaken for something else.
        if in_block:
            m = _BLOCK_ENTRY.match(line)
            if m:
                run.blamed_entries.append(m.group(1))
                continue
            in_block = False
        if _FAILING_TESTS_HEADER.match(line):
            in_block = True
            continue

        if _HOST_RESTART.search(line):
            run.host_restarts += 1
            if run.restart_evidence is None:
                run.restart_evidence = line.strip()
            continue

        # --- Swift Testing -------------------------------------------------
        m = _ST_ISSUE_NAMED.search(line) or _ST_ISSUE_FUNC.search(line)
        if m:
            name, source, message = m.group(1), m.group(3), m.group(6)
            failure = failure_for(st_key(name), FRAMEWORK_SWIFT_TESTING, name)
            failure.kinds.add(_kind_of_issue(message))
            if source and not failure.source:
                failure.source = source
            continue

        m = _ST_FAIL_NAMED.search(line) or _ST_FAIL_FUNC.search(line)
        if m:
            name = m.group(1)
            failure = failure_for(st_key(name), FRAMEWORK_SWIFT_TESTING, name)
            # None when the line was truncated before its duration. The FAILURE
            # is what matters and is already recorded; a missing duration must
            # never discard it.
            if m.group(2) is not None:
                seconds = float(m.group(2))
                if failure.seconds is None or seconds > failure.seconds:
                    failure.seconds = seconds
            continue

        m = _ST_PASS_NAMED.search(line) or _ST_PASS_FUNC.search(line)
        if m:
            run.passed.add(st_key(m.group(1)))
            continue

        m = _ST_SKIP_NAMED.search(line) or _ST_SKIP_FUNC.search(line)
        if m:
            run.skipped.add(st_key(m.group(1)))
            continue

        m = _ST_START_NAMED.search(line) or _ST_START_FUNC.search(line)
        if m:
            run.started.add(st_key(m.group(1)))
            continue

        # --- XCTest --------------------------------------------------------
        m = _XC_RESULT.search(line)
        if m:
            suite, method, outcome, seconds = m.groups()
            key = xc_key(suite, method)
            if outcome == "skipped":
                run.skipped.add(key)
            elif outcome == "failed":
                failure = failure_for(key, FRAMEWORK_XCTEST, suite + "/" + method)
                # XCTest reports assertions, never time-limit issues. A 3.5s
                # XCTest failure is as real as a 0.025s one — the slow-is-a-flake
                # heuristic belongs to Swift Testing alone and must never be
                # applied here.
                failure.kinds.add(KIND_ASSERTION)
                if seconds is not None:
                    failure.seconds = float(seconds)
            else:
                run.passed.add(key)
            continue

        m = _XC_START.search(line)
        if m:
            run.started.add(xc_key(m.group(1), m.group(2)))

    for failure in failures.values():
        if not failure.kinds:
            failure.kinds.add(KIND_UNKNOWN)

    # Swift Testing's console line carries no suite name, so two same-named
    # tests in different suites collide on one key. Resolve toward FAILED: a
    # colliding pass must never erase a real failure.
    run.passed -= set(failures)
    # Same rule one step further out: a skip colliding with a real outcome must
    # never swallow it. The residue is unchanged and is the one already written
    # down above — two same-named tests share a key, so a skipped twin still
    # accounts for a SILENT twin. That is the 0.57%-of-names collision cost,
    # not a new hole.
    #
    # THIS LINE IS A NO-OP TODAY AND IS KEPT AS AN INVARIANT, not as behaviour
    # — R1 review's mutation rail M23 survived deleting it and was called a
    # proven equivalent mutant on the evidence of two logs rendering
    # identically. R2 review checked the claim instead of accepting it, and it
    # is true for EVERY input, which is a stronger statement than two logs:
    # `run.passed` has already had `failures` removed one line up, so the set
    # subtracted here is exactly `ran`. Both of the only two readers of
    # `run.skipped` — `no_verdict`, which subtracts `ran` first, and
    # `identities`, which unions `ran` in — are indifferent to whether a member
    # of `ran` is also in `skipped`. What the line buys is that a THIRD reader
    # cannot be written against a `skipped` set that overlaps `ran`; delete it
    # only together with that guarantee.
    run.skipped -= set(failures) | run.passed
    return run


# ---------------------------------------------------------------------------
# The baseline file
# ---------------------------------------------------------------------------

# playhead-buvn / playhead-tl6l R4. The recorded crashed-host census, shaped
# EXACTLY like `tests`: a mapping of key -> observation counts, unioned across
# accepts, with the tier falling out of the counts rather than a hand label.
#
#   "no_verdict": {
#     "runs_observed": 3,
#     "tests": {"swift-testing::clearCache …": {"seen_runs": 3, "lost_runs": 3}}
#   }
#
# THREE STATES, THREE DIFFERENT CLAIMS, and they must never be spelled the same
# way — reading an absence as a measurement is this repo's standing defect
# class, and this key has now been the site of it twice:
#
#   * the key is ABSENT   — nobody has ever recorded the population. The arm is
#     INERT and the verdict says so out loud. This is what let the arming land
#     without turning main red for a pre-existing crash owned by playhead-rouw;
#   * 1..2 observations   — PROVISIONAL. Measured, but not enough observations
#     to bound a population this load-sensitive (see below). A casualty nobody
#     recorded is NAMED and forecloses GREEN; it does not fail the gate;
#   * >= MIN_RUNS_FOR_DETERMINISTIC observations — ARMED.
NO_VERDICT_KEY = "no_verdict"
CENSUS_RUNS_KEY = "runs_observed"
CENSUS_TESTS_KEY = "tests"


class Census(object):
    """The recorded crashed-host population, with its observation counts."""

    def __init__(self, runs_observed=0, tests=None, legacy=False):
        self.runs_observed = runs_observed
        self.tests = dict(tests or {})
        # True when the record was read in the pre-R4 shape (a bare list). It
        # carries no counts, so it is read as the WEAKEST true claim it
        # supports — one observation — which lands it in PROVISIONAL and arms
        # nothing off numbers nobody measured.
        self.legacy = legacy

    @property
    def names(self):
        return set(self.tests)

    @property
    def armed(self):
        return self.runs_observed >= MIN_RUNS_FOR_DETERMINISTIC

    def tier(self, key):
        return census_tier_of(self.tests[key])

    def to_json(self):
        return {CENSUS_RUNS_KEY: self.runs_observed,
                CENSUS_TESTS_KEY: {key: dict(entry)
                                   for key, entry in sorted(self.tests.items())}}


def empty_baseline(plan):
    """A file nobody has accepted anything into yet.

    It carries NO census key on purpose. `{"runs_observed": 0, "tests": {}}`
    would spell "nobody has looked" as a measurement of zero, which is the
    exact conflation the three states above exist to keep apart. A census with
    zero observations is only ever produced by writing one by hand.
    """
    return {"plan": plan, "mode": "full-plan", "runs_observed": 0, "tests": {}}


def recorded_census(baseline):
    """The recorded census, or None when the population was never recorded."""
    value = baseline.get(NO_VERDICT_KEY)
    if value is None:
        return None
    if isinstance(value, list):
        # The pre-R4 shape: a bare set of names, replaced on each accept. No
        # instance was ever committed, but reading one silently as "armed"
        # would arm a pass-direction check on counts that do not exist.
        return Census(1, {key: {"seen_runs": 1, "lost_runs": 1} for key in value},
                      legacy=True)
    return Census(value.get(CENSUS_RUNS_KEY, 0), value.get(CENSUS_TESTS_KEY, {}))


def recorded_no_verdict(baseline):
    """Just the NAMES, or None when the population was never recorded."""
    census = recorded_census(baseline)
    return None if census is None else census.names


def load_baseline(path):
    with open(str(path), encoding="utf-8") as handle:
        return json.load(handle)


def save_baseline(path, data):
    path = pathlib.Path(str(path))
    payload = json.dumps(data, indent=2, sort_keys=True, ensure_ascii=False)
    path.write_text(payload + "\n", encoding="utf-8")


def _tier(seen_runs, hit_runs):
    """The tier rule itself, over a numerator and a denominator.

    Shared by the failure baseline (`failed_runs` / `seen_runs`) and the
    crashed-host census (`lost_runs` / `seen_runs`) so there is ONE definition
    of "deterministic" in this file rather than two that can drift. It takes
    the two numbers rather than an entry precisely so neither caller can pass
    the other's field by accident — the fields are deliberately NOT spelled the
    same, because a value that names one thing read as though it named another
    is the defect class this whole module keeps tripping over.
    """
    return (TIER_DETERMINISTIC
            if seen_runs >= MIN_RUNS_FOR_DETERMINISTIC and hit_runs == seen_runs
            else TIER_LOAD_SENSITIVE)


def tier_of(entry):
    return _tier(entry["seen_runs"], entry["failed_runs"])


def census_tier_of(entry):
    """The same rule on a census entry: lost its verdict in every observation."""
    return _tier(entry["seen_runs"], entry["lost_runs"])


def tier_changes(baseline, merged):
    """Which entries changed TIER in this merge: `(promoted, demoted)`.

    Promotion is the one that matters. Crossing into `deterministic` ARMS the
    pass-direction check — from that merge onward the entry PASSING hard-fails
    the gate — and until playhead-26od R5 an accept reported only membership,
    so an accept could arm fifteen hard failures and print nothing about any of
    them. Arming a failure silently is the single thing this module exists not
    to do; the whole point of `--accept-baseline` is that a human writes down
    what changed, and a human cannot write down what they were never shown.

    Pure, so the CLI only has to print it. A newly ADDED entry can never appear
    here: it enters at `seen_runs = 1`, which is below
    `MIN_RUNS_FOR_DETERMINISTIC` by construction, so promotion always takes at
    least one further observation and is always a CHANGE to something already
    recorded.
    """
    old = baseline.get("tests", {}) if baseline.get("plan") == merged.get("plan") else {}
    promoted = []
    demoted = []
    for key, entry in merged.get("tests", {}).items():
        now = tier_of(entry)
        before = tier_of(old[key]) if key in old else None
        if now == TIER_DETERMINISTIC and before != TIER_DETERMINISTIC:
            promoted.append(key)
        elif before == TIER_DETERMINISTIC and now != TIER_DETERMINISTIC:
            demoted.append(key)
    return sorted(promoted), sorted(demoted)


def _kinds_label(entry):
    """`timeout`, `assertion`, `assertion+timeout` — never blank."""
    return "+".join(sorted(entry.get("kinds", []))) or KIND_UNKNOWN


def kind_census(entries):
    """`{kinds-label: count}` over a list of baseline entries.

    Exists because of what it would have caught. playhead-26od's third accept
    added 28 entries and was justified in the commit message as "all timeouts";
    three were assertion-only and a fourth mixed, which under this module's own
    identity rule ("a load-sensitive entry means MAY TIME OUT") is a different
    claim entirely. Nothing in the accept output named a single entry's kind, so
    the summary was written from the count. Print the census and it cannot be.
    """
    census = {}
    for entry in entries:
        label = _kinds_label(entry)
        census[label] = census.get(label, 0) + 1
    return census


def merge_census(prior, run):
    """Fold one run's crashed-host observations into the recorded census.

    UNION WITH COUNTS, not a replaced set, and the difference is a measurement
    rather than a preference — see the module docstring for the three-run
    numbers. Each recorded name is credited an observation whenever the run
    REACHED it, which for this population means "emitted a start line":

      * lost its verdict again -> seen + 1, lost + 1;
      * started and reported   -> seen + 1, lost unchanged. This is the only
        thing that can DEMOTE an entry out of `deterministic`, and it is what
        keeps a union from ossifying into a licence nobody can revoke;
      * never started at all   -> ambiguous, and the tie is broken by whether
        this run's host died. On a healthy run it is a rename, a deletion or a
        new skip, and the entry is PRUNED. On a crashed one it is exactly what
        the crash does, so the entry is CARRIED FORWARD at its original counts
        and no observation is credited. Pruning there is how a crash would
        shrink the record from inside the one command meant to maintain it —
        the same defect `protected` closes for `tests`, one layer down.
    """
    prior = prior or Census()
    lost = run.no_verdict
    # Deliberately NOT `run.blamed_entries`. xcodebuild prints a `Failing
    # tests:` block on every run that has any failure at all, so counting it as
    # crash evidence would make `crashed` true on essentially every full-plan
    # run and disable the prune entirely — the record would then never shrink
    # for a rename, which is the other half of what makes a union affordable.
    crashed = bool(run.host_restarts or lost)
    tests = {}
    for key in sorted(set(prior.tests) | lost):
        previous = prior.tests.get(key)
        if previous is None:
            tests[key] = {"seen_runs": 1, "lost_runs": 1}
        elif key in lost:
            tests[key] = {"seen_runs": previous["seen_runs"] + 1,
                          "lost_runs": previous["lost_runs"] + 1}
        elif key in run.started:
            tests[key] = {"seen_runs": previous["seen_runs"] + 1,
                          "lost_runs": previous["lost_runs"]}
        elif crashed:
            tests[key] = dict(previous)
    return Census(prior.runs_observed + 1, tests)


def census_changes(prior, merged):
    """`(added, reported_again, dropped)` between two censuses, for the accept.

    Pure, so the CLI only has to print it. An accept is a claim a human signs
    in a commit message, and under a union the interesting event is no longer
    "the list was replaced" — it is which names ENTERED, which ones came back
    (and so lost ground toward deterministic), and which ones the prune
    dropped.
    """
    before = prior.tests if prior else {}
    added = sorted(set(merged.tests) - set(before))
    dropped = sorted(set(before) - set(merged.tests))
    reported = sorted(
        key for key in merged.tests
        if key in before
        and merged.tests[key]["lost_runs"] == before[key]["lost_runs"]
        and merged.tests[key]["seen_runs"] > before[key]["seen_runs"]
    )
    return added, reported, dropped


def census_tier_changes(prior, merged):
    """Which census entries changed TIER in this merge: `(promoted, demoted)`.

    Same reason `tier_changes` exists for failures, and now the same SHAPE —
    both directions, one signature — because R1 of playhead-o89d fixed the
    silent-demotion asymmetry on the `tests` side and left the identical one
    here, one layer down. Crossing INTO `deterministic` arms a hard failure
    (from here REPORTING AGAIN fails the gate). Crossing OUT of it revokes that
    licence, and a licence revoked silently is the same defect as one armed
    silently — it is just pointed the other way, which is the direction that
    makes the gate LOOSER.

    A census demotion has exactly one cause, and `merge_census` names it: an
    entry the run STARTED AND REPORTED gets `seen + 1` with `lost` unchanged.
    That is the same event as the verdict's `census_now_reports`, i.e. the one
    that hard-failed the run being accepted.
    """
    before = prior.tests if prior else {}

    def was_deterministic(key):
        return key in before and census_tier_of(before[key]) == TIER_DETERMINISTIC

    promoted = []
    demoted = []
    for key, entry in merged.tests.items():
        now = census_tier_of(entry) == TIER_DETERMINISTIC
        if now and not was_deterministic(key):
            promoted.append(key)
        elif was_deterministic(key) and not now:
            demoted.append(key)
    return sorted(promoted), sorted(demoted)


def merge(baseline, run, plan):
    """Fold one run's observations into the baseline and return the new file.

    Self-pruning by construction: an entry the run never reached is dropped
    (renamed, deleted or newly skipped), and an entry that has failed in none of
    its observations is dropped (fixed). Both directions shrink the file without
    anyone editing it, which is what keeps the pass-direction arm affordable.
    """
    if not run.complete:
        raise CannotEvaluate(
            "the log has no terminal verdict — it is a fragment, not a run"
        )

    # A run can carry a terminal verdict and still have executed NOTHING.
    # Measured: erasing the simulator made xcodebuild reach for the clone
    # helper, which resolves `simctl` through the GLOBAL xcode-select
    # (CommandLineTools, no simctl); it printed `** TEST FAILED **` after zero
    # tests. Accepting that would silently DELETE every entry as unreachable and
    # call the empty result a baseline — the file destroyed by the very command
    # meant to maintain it.
    if not run.ran:
        raise CannotEvaluate(
            "the run recorded no test results at all — it did not exercise the plan"
        )
    existing = set(baseline.get("tests", {})) if baseline.get("plan") == plan else set()
    if existing:
        reached = len(existing & run.ran)
        if reached * 2 < len(existing):
            raise CannotEvaluate(
                "the run reached only %d of the %d recorded tests — too few to be a "
                "run of this plan, and accepting it would drop the rest"
                % (reached, len(existing))
            )

    if baseline.get("plan") != plan:
        # A different plan is a different population. Carrying entries across
        # would name tests the new plan never runs, which reads as ABSENT
        # forever.
        baseline = empty_baseline(plan)

    merged = {
        "plan": plan,
        "mode": baseline.get("mode", "full-plan"),
        "runs_observed": baseline.get("runs_observed", 0) + 1,
        "tests": {},
        # playhead-tl6l R4. UNIONED WITH OBSERVATION COUNTS, exactly like
        # `tests` above, and for the same measured reason: the population
        # churns. Two runs said it did not — Jaccard 1.00, which is what R2
        # replaced a union with a set on — and the THIRD run, a real full-plan
        # gate on this branch, lost 15 where those two lost 11. Same eleven all
        # three times, four more on the loud night, and nothing ever recovered.
        # Two observations could not have shown that, which is the whole
        # argument for counting observations rather than replacing a snapshot.
        #
        # It is still a set of NAMES, never a count, for R2's reason, which the
        # third run does not touch: a count cannot see substitution — eleven
        # tests die, eleven different ones recover, the total is unchanged.
        NO_VERDICT_KEY: merge_census(recorded_census(baseline), run).to_json(),
    }

    old = baseline.get("tests", {})
    # playhead-tl6l. A crashed host is not a rename, and the prune below cannot
    # tell them apart on its own — so a run whose host died would have DELETED
    # every recorded entry it took down with it, quietly, from inside the one
    # command whose job is to maintain the file. Carry those forward untouched:
    # unchanged counts, no observation credited, still on the list.
    protected = run.no_verdict
    for key in sorted(set(old) | set(run.failures)):
        previous = old.get(key)
        failure = run.failures.get(key)
        reached = key in run.ran

        if previous is None:
            merged["tests"][key] = {
                "framework": failure.framework,
                "name": failure.name,
                "seen_runs": 1,
                "failed_runs": 1,
                "kinds": sorted(failure.kinds),
                "source": failure.source,
            }
            continue

        if not reached:
            if key in protected:
                merged["tests"][key] = dict(previous)
            continue  # renamed, deleted or skipped — it is not knowledge

        entry = dict(previous)
        entry["seen_runs"] = previous["seen_runs"] + 1
        if failure is not None:
            entry["failed_runs"] = previous["failed_runs"] + 1
            entry["kinds"] = sorted(set(previous.get("kinds", [])) | failure.kinds)
            if failure.source:
                entry["source"] = failure.source
        if entry["failed_runs"] == 0:
            continue  # never failed in any observation — it is fixed
        merged["tests"][key] = entry

    return merged


# ---------------------------------------------------------------------------
# The verdict
# ---------------------------------------------------------------------------

class Verdict(object):
    def __init__(self):
        self.cannot_evaluate = None
        self.new_failures = []
        self.kind_changed = []
        self.kind_detail = {}
        self.deterministic_passed = []
        self.load_sensitive_passed = []
        self.absent = []
        self.baseline_fiction = False
        self.known_failures = []
        self.runs_observed = 0
        self.total_failures = 0
        self.baseline_size = 0
        # playhead-tl6l — the run's SILENCE, tracked as its own thing.
        self.no_verdict = []
        # playhead-buvn — and diffed against the record, like everything else.
        # None means the population has never been recorded, which is NOT the
        # same as a recorded empty set. See NO_VERDICT_KEY.
        self.no_verdict_recorded = None
        self.new_casualties = []
        self.recovered_casualties = []
        # playhead-tl6l R4 — the record now carries observation counts, so the
        # verdict can say which of the three states it is in and which
        # recoveries are evidence rather than weather.
        self.census_runs_observed = 0
        self.census_armed = False
        self.census_legacy = False
        # Recorded names that came back AND demonstrably started this run,
        # whose entry claims they lose their verdict every single time. The
        # pass-direction arm, on the only population where it is sound.
        self.census_now_reports = []
        # Recorded names this run demonstrably STARTED — the only ones about
        # which a recovery is evidence rather than an absence.
        self.census_started = set()
        self.absent_crashed = set()
        self.host_restarts = 0
        self.restart_evidence = None
        self.blamed_entry_count = 0
        self.blamed_distinct = []
        self.blamed_unmatched = []

    @property
    def ok(self):
        return self.exit_code == EXIT_OK

    @property
    def crashed_host(self):
        """Did this run fail to produce a verdict for part of the plan?

        Any one of the three is enough, and they are independent evidence:
        xcodebuild said it restarted the host; tests started and said nothing;
        or the summary block blamed a name that never emitted a console line.
        """
        return bool(self.host_restarts or self.no_verdict or self.blamed_unmatched)

    @property
    def headline_tail(self):
        """What rides on the RED line. Says only what was actually observed.

        Three different observations, three different sentences — a headline
        reading `0 tests got NO VERDICT` because some OTHER piece of evidence
        fired would be precisely the number that means one thing and is read as
        another.
        """
        if self.no_verdict:
            # playhead-buvn: the count alone is not what turned the gate red, so
            # the headline must not imply it did. A run reading `RED (85 known /
            # 0 new) — 12 tests got NO VERDICT` and exiting 65 sends the reader
            # to hunt a regression in a category that says `0 new`; the fatal
            # fact is that ONE of the twelve is not in the record, and that is
            # the number the first line has to carry.
            # Case carries signal here as it does in the known/new split: the
            # capitals are the shout, and an unrecorded casualty on a
            # PROVISIONAL record is reported without being fatal, so it must
            # not be shouted.
            if self.new_casualties and self.census_armed:
                unrecorded = ", %d NOT RECORDED" % len(self.new_casualties)
            elif self.new_casualties:
                unrecorded = ", %d not yet recorded" % len(self.new_casualties)
            else:
                unrecorded = ""
            return " — %d test%s got NO VERDICT (crashed host)%s" % (
                len(self.no_verdict), "" if len(self.no_verdict) == 1 else "s",
                unrecorded,
            )
        if self.host_restarts:
            return " — the test host CRASHED and was restarted"
        if self.blamed_unmatched:
            return " — %d name(s) in `Failing tests:` matched no console result" % (
                len(self.blamed_unmatched),
            )
        return ""

    @property
    def exit_code(self):
        if self.cannot_evaluate:
            return EXIT_CANNOT_EVALUATE
        # playhead-buvn. NO VERDICT is armed the same way every other category
        # in this module is armed: not on the COUNT, on the DIFF against the
        # record. A name that lost its verdict and is not in the record is a
        # NEW CASUALTY and fails the gate — which is what closes the one
        # regression class tl6l left open, a change that CRASHES the test host
        # and so destroys the evidence of itself (its victims are healthy tests
        # in nobody's baseline, so `new_failures` and `absent` are both empty
        # and the run used to exit 0).
        #
        # The count ALONE is still not fatal, deliberately, and neither is a
        # bare host restart: a run at or under the record is the pre-existing
        # crash playhead-rouw owns, and a gate red for a reason its reader
        # cannot fix is one they learn to route around. A restart that costs
        # nobody a verdict means every test got re-run and no information was
        # lost — it is reported, it forecloses GREEN, and it stops there.
        #
        # TWO CONDITIONS ARMED, AND THE PAIR IS WHAT MAKES A UNION SAFE
        # (playhead-tl6l R4):
        #
        #   * a NEW CASUALTY, once the record is ARMED. Not while it is
        #     PROVISIONAL: two observations do not bound a load-sensitive
        #     population, and this one was measured not to be bounded by them
        #     — run 3 lost four names runs 1 and 2 had both seen PASS. A
        #     newly-observed casualty is indistinguishable from a regression,
        #     which is why it is named on every run from the first; what three
        #     observations buy is the right to make it fatal;
        #   * a DETERMINISTIC entry that REPORTED AGAIN. Its record claims it
        #     loses its verdict in every observation and it did not, so the
        #     licence has rotted and must be revoked. This is Dan's
        #     pass-direction call applied where it is sound, and it is the
        #     price of the union: without it a crash that gets genuinely fixed
        #     stays recorded forever and the gate never speaks about those
        #     names again, which is R2's whole objection to unioning.
        #
        # A LOAD-SENSITIVE entry reporting again is neither — one quiet run is
        # not evidence a crash is fixed, exactly as for a load-sensitive
        # failure that passes.
        if (self.new_failures or self.kind_changed or self.deterministic_passed
                or self.absent or self.baseline_fiction
                or (self.new_casualties and self.census_armed)
                or self.census_now_reports):
            return EXIT_REGRESSION
        return EXIT_OK

    def render(self):
        out = []
        if self.cannot_evaluate:
            out.append("gate-baseline: CANNOT EVALUATE — " + self.cannot_evaluate)
            out.append(
                "  The gate's own exit code stands; this check made no claim."
            )
            return "\n".join(out)

        # Case carries signal, exactly as the bead specifies it: "0 new" is the
        # quiet all-clear, "2 NEW" is the shout. Someone skimming a CI log reads
        # the capitals before they read the number.
        headline = "%d known / %d %s" % (
            len(self.known_failures),
            len(self.new_failures),
            "NEW" if self.new_failures else "new",
        )
        # playhead-tl6l: the count rides ON the headline, not under it. The whole
        # hazard was that a crashed run could print the reassuring `RED (N known
        # / 0 new)` — the exact string CLAUDE.md tells people to read as an
        # all-clear — while an entire test family never reached a verdict. That
        # string can no longer stand alone.
        tail = self.headline_tail
        # GREEN is reserved for "nothing failed AND nothing else is wrong". A run
        # that executed no tests has zero failures too, and calling that GREEN is
        # how a broken run reads as a clean sweep. A run that lost part of the
        # plan to a dead host is the same claim with the same answer.
        if self.ok and self.total_failures == 0 and not self.crashed_host:
            out.append("gate-baseline: GREEN (%s)" % headline)
        else:
            out.append("gate-baseline: RED (%s)%s" % (headline, tail))

        out.extend(self._render_no_verdict())

        for key in self.new_failures:
            out.append("  NEW FAILURE      %s" % key)
        for key in self.kind_changed:
            out.append("  FAILS DIFFERENTLY %s — %s" % (key, self.kind_detail.get(key, "")))
        for key in self.deterministic_passed:
            out.append("  NOW PASSES       %s  (recorded as failing every run)" % key)
        if self.absent:
            out.append(
                "  DID NOT RUN — %d of %d recorded tests were never reached. If that "
                "is most of them, the run did not exercise the plan (a wedged "
                "simulator or a failed install reports `** TEST FAILED **` after "
                "zero tests)." % (len(self.absent), self.baseline_size)
            )
        for key in self.absent[:_MAX_LISTED]:
            # The CAUSE, not a guess at it. Before playhead-tl6l every absent
            # member was reported as a rename, which sent the reader looking for
            # a rename that had not happened.
            cause = ("no verdict — the host died mid-test"
                     if key in self.absent_crashed
                     else "renamed, deleted or newly skipped")
            out.append("  DID NOT RUN      %s  (%s)" % (key, cause))
        if len(self.absent) > _MAX_LISTED:
            out.append("  DID NOT RUN      … and %d more"
                       % (len(self.absent) - _MAX_LISTED))
        if self.baseline_fiction:
            out.append(
                "  BASELINE IS FICTION — the run had zero failures while %d are "
                "recorded as known-broken." % len(self.known_failures)
            )
        for key in self.load_sensitive_passed:
            out.append("  (passed this run, load-sensitive, removal candidate) %s" % key)

        if self.runs_observed < MIN_RUNS_FOR_DETERMINISTIC:
            out.append(
                "  NOTE: the baseline is unconfirmed — built from %d observation(s). "
                "Nothing can be classed deterministic, so the pass-direction arm is "
                "inert until a second `--accept-baseline` run." % self.runs_observed
            )

        if self.exit_code != EXIT_OK:
            out.append("")
            out.append(
                "The gate is RED for a reason that is NOT in the baseline. Fix it, or —"
            )
            out.append(
                "if the change is intended — refresh the record with "
                "`scripts/fast-gate.sh --accept-baseline` and justify the diff in the"
            )
            out.append(
                "commit message. A shrinking baseline is good news; a growing one needs"
            )
            out.append("a reason.")
        return "\n".join(out)

    def _render_no_verdict(self):
        """The crashed-host block. Its own category, because its REMEDY differs.

        A NEW failure is triaged against the diff. A test with no verdict was
        never judged at all, and the only honest response is to run it again —
        so it must not be folded into NEW, where it would read as something a
        reader could act on by looking at their own change.
        """
        if not self.crashed_host:
            # Nothing was lost — but the record may still say something WAS,
            # and a shrinking census is the good news this file exists to make
            # visible. Report it on its own rather than swallowing it with the
            # crash block it no longer belongs to.
            return self._render_casualty_diff() if self.recovered_casualties else []
        out = []
        if self.no_verdict:
            out.extend([
                "  NO VERDICT — %d test(s) started and then reported neither pass, "
                "fail nor skip." % len(self.no_verdict),
                "  The run made NO CLAIM about them: they count as neither known nor "
                "NEW, so the",
                "  known/new split above is a verdict about the REST of the plan. "
                "Re-run before",
                "  reading it as one.",
            ])
        else:
            out.append(
                "  NO VERDICT — every test that started reported an outcome, but the "
                "evidence below says part of this run was still lost. Read it before "
                "reading the split above."
            )
        if self.host_restarts:
            out.append(
                "  HOST RESTART     xcodebuild restarted the test host %d time(s): %r"
                % (self.host_restarts, self.restart_evidence or "")
            )
        out.extend(self._render_casualty_diff())
        for key in self.no_verdict[:_MAX_LISTED]:
            out.append("  NO VERDICT       %s" % key)
        if len(self.no_verdict) > _MAX_LISTED:
            out.append("  NO VERDICT       … and %d more"
                       % (len(self.no_verdict) - _MAX_LISTED))
        if self.blamed_distinct:
            out.append(
                "  BLAMED           xcodebuild's `Failing tests:` summary: %d entries, "
                "%d distinct name(s); %d matched no console line in any spelling."
                % (self.blamed_entry_count, len(self.blamed_distinct),
                   len(self.blamed_unmatched))
            )
            out.append(
                "                   A repeated entry is a RETRY, not a second test. "
                "The unmatched list is a LEAD, not a count: the summary spells a test "
                "`Suite.function()` while Swift Testing's console prints its @Test "
                "display name, so a display-named test that reported perfectly well "
                "is unmatched here too. Cross-check it against the census above."
            )
            for name in self.blamed_unmatched[:_MAX_LISTED]:
                out.append("  BLAMED, UNMATCHED %s" % name)
            if len(self.blamed_unmatched) > _MAX_LISTED:
                out.append("  BLAMED, UNMATCHED … and %d more"
                           % (len(self.blamed_unmatched) - _MAX_LISTED))
        return out

    def _render_casualty_diff(self):
        """playhead-buvn: the census against its record, in both directions."""
        if self.no_verdict_recorded is None:
            return [
                "  NOT RECORDED     this baseline has never recorded a crashed-host "
                "census, so the arm below is INERT: a run that loses MORE tests than "
                "this one would still exit 0.",
                "                   `scripts/fast-gate.sh --accept-baseline` records "
                "the set and arms it. Recording ZERO is a real claim and is armed too; "
                "recording NOTHING, which is where this baseline is, is not.",
            ]
        out = []
        if self.census_legacy:
            out.append(
                "  OLD CENSUS SHAPE the record is a bare list of names with no "
                "observation counts. It is read as ONE observation — the weakest "
                "claim it supports — so nothing is armed off numbers nobody "
                "measured. `--accept-baseline` rewrites it."
            )
        if not self.census_armed:
            out.append(
                "  PROVISIONAL      the census has %d observation(s); it takes %d "
                "before an unrecorded casualty can fail the gate. Two runs said this "
                "population was stable (Jaccard 1.00) and the third lost four more "
                "names that had PASSED in both — so a casualty nobody has recorded "
                "yet is named here and is not fatal."
                % (self.census_runs_observed, MIN_RUNS_FOR_DETERMINISTIC)
            )
        # Truncated like every other category. A change that kills the host can
        # take down hundreds at once, and a verdict nobody reads to the end is a
        # verdict nobody acts on — which is the note at _MAX_LISTED.
        for key in self.new_casualties[:_MAX_LISTED]:
            out.append("  NEW CASUALTY     %s  (lost its verdict; not in the record)"
                       % key)
        if len(self.new_casualties) > _MAX_LISTED:
            out.append("  NEW CASUALTY     … and %d more"
                       % (len(self.new_casualties) - _MAX_LISTED))
        if self.new_casualties:
            out.append(
                "                   These are the crash's own witnesses. A change that "
                "kills the test host takes down HEALTHY tests, which are in nobody's "
                "failure baseline — so this is the only arm that can see it. Re-run to "
                "separate a real regression from a worse day on the box; if the loss is "
                "genuinely pre-existing, record it."
            )
        for key in self.census_now_reports[:_MAX_LISTED]:
            out.append("  NOW REPORTS      %s  (recorded as losing its verdict in "
                       "EVERY observation)" % key)
        if len(self.census_now_reports) > _MAX_LISTED:
            out.append("  NOW REPORTS      … and %d more"
                       % (len(self.census_now_reports) - _MAX_LISTED))
        if self.census_now_reports:
            out.append(
                "                   Good news that is nonetheless FATAL, and for the "
                "same reason a deterministic failure passing is: the record says these "
                "lose their verdict every single time and they did not, so it has "
                "rotted. Refresh it with `--accept-baseline` — that is what lets a "
                "fixed crash leave a record that otherwise only grows."
            )
        rest = [key for key in self.recovered_casualties
                if key not in set(self.census_now_reports)]
        for key in rest[:_MAX_LISTED]:
            # The CAUSE, not a guess at it. A name that started and reported is
            # positive evidence; a name that never started at all is a rename,
            # a deletion, or a host that died before it got there — three
            # things this module cannot tell apart, so it never arms on them.
            cause = ("recorded as losing its verdict; reported again — removal "
                     "candidate" if key in self.census_started
                     else "recorded as losing its verdict; did not start at all this "
                          "run, so this says nothing either way")
            out.append("  REPORTED AGAIN   %s  (%s)" % (key, cause))
        if len(rest) > _MAX_LISTED:
            out.append("  REPORTED AGAIN   … and %d more" % (len(rest) - _MAX_LISTED))
        if rest:
            out.append(
                "                   NOT fatal: one quiet run is not evidence a crash "
                "is fixed, which is the same reason a load-sensitive failure passing "
                "is a candidate rather than a verdict. Shrink the record with "
                "`--accept-baseline` when you believe it."
            )
        return out


def _blamed_is_matched(entry, identities):
    """Can this `Failing tests:` entry be tied to a console identity BY NAME?

    Only two spellings are ever comparable, and both are exact — no fuzzy
    matching, because a false match here hides a casualty and a false miss
    manufactures one:

      * XCTest, whose console key is `Target.Suite/method`; and
      * a Swift Testing test with NO custom display name, whose console line
        prints the bare `function()`.

    A Swift Testing test WITH a display name is unmatchable in principle and is
    reported as such rather than guessed at. See the module docstring.
    """
    m = _BLOCK_NAME.match(entry)
    if not m:
        return False
    suite, method = m.group(1), m.group(2)
    if st_key(method + "()") in identities:
        return True
    tail = "." + suite + "/" + method
    return any(key.startswith(FRAMEWORK_XCTEST + "::")
               and (key.endswith(tail) or key == xc_key(suite, method))
               for key in identities)


def verdict(baseline, run, plan=None):
    """Compare one run against the baseline. Pure; the CLI only prints it."""
    result = Verdict()
    result.runs_observed = baseline.get("runs_observed", 0)
    result.total_failures = len(run.failures)
    result.baseline_size = len(baseline.get("tests", {}))

    if plan is not None and baseline.get("plan") != plan:
        result.cannot_evaluate = (
            "the baseline was recorded for plan '%s' but this run is '%s'. "
            "Different plans run different populations." % (baseline.get("plan"), plan)
        )
        return result

    if not run.complete:
        result.cannot_evaluate = (
            "the log is incomplete — no terminal verdict, so every test after the "
            "cut looks like it never ran"
        )
        return result

    # playhead-tl6l. Computed before the baseline comparison so the ABSENT arm
    # below can name the CAUSE it now knows.
    no_verdict = run.no_verdict
    result.no_verdict = sorted(no_verdict)
    result.host_restarts = run.host_restarts
    result.restart_evidence = run.restart_evidence
    # playhead-buvn. The DIFF against the recorded census, computed exactly the
    # way `new_failures` is: a name nobody recorded is a regression, and a
    # recorded name that came back is reported as good news but is not fatal —
    # one quiet run is not proof a crash is fixed, and the removal is the
    # operator's to make with `--accept-baseline`.
    census = recorded_census(baseline)
    recorded = None if census is None else census.names
    result.no_verdict_recorded = recorded
    if recorded is not None:
        result.census_runs_observed = census.runs_observed
        result.census_armed = census.armed
        result.census_legacy = census.legacy
        result.new_casualties = sorted(no_verdict - recorded)
        result.recovered_casualties = sorted(recorded - no_verdict)
        # playhead-tl6l R4. The pass-direction arm, and it fires only on
        # POSITIVE evidence: the entry claims this test loses its verdict in
        # every observation, and this run watched it start and report. A
        # recorded name that never started is a rename, a deletion or a host
        # that died earlier — indistinguishable from here, so never fatal.
        result.census_started = recorded & run.started
        result.census_now_reports = sorted(
            key for key in result.recovered_casualties
            if key in result.census_started
            and census.tier(key) == TIER_DETERMINISTIC
        )

    blamed = run.blamed
    result.blamed_entry_count = len(run.blamed_entries)
    result.blamed_distinct = blamed
    identities = run.ran | run.passed | run.skipped | run.started
    result.blamed_unmatched = [name for name in blamed
                               if not _blamed_is_matched(name, identities)]

    entries = baseline.get("tests", {})

    for key in sorted(run.failures):
        failure = run.failures[key]
        entry = entries.get(key)
        if entry is None:
            result.new_failures.append(key)
            continue
        known = set(entry.get("kinds", []))
        unexpected = failure.kinds - known
        if unexpected:
            result.kind_changed.append(key)
            result.kind_detail[key] = "recorded as %s, failed as %s" % (
                "/".join(sorted(known)) or "?", "/".join(sorted(unexpected))
            )
        else:
            result.known_failures.append(key)

    for key in sorted(entries):
        if key in run.failures:
            continue
        if key in run.passed:
            if tier_of(entries[key]) == TIER_DETERMINISTIC:
                result.deterministic_passed.append(key)
            else:
                result.load_sensitive_passed.append(key)
        else:
            result.absent.append(key)
            if key in no_verdict:
                result.absent_crashed.add(key)

    if entries and not run.failures:
        result.baseline_fiction = True

    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _read(path):
    return pathlib.Path(str(path)).read_text(encoding="utf-8", errors="replace")


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="gate_baseline.py",
        description="Judge a gate run against the committed baseline of known failures.",
    )
    sub = parser.add_subparsers(dest="command")

    for name in ("check", "accept"):
        p = sub.add_parser(name)
        p.add_argument("--log", required=True)
        p.add_argument("--baseline", required=True)
        p.add_argument("--plan", default=None)

    args = parser.parse_args(argv)
    if not args.command:
        parser.print_help()
        return EXIT_CANNOT_EVALUATE

    log_path = pathlib.Path(args.log)
    if not log_path.exists():
        sys.stderr.write("gate-baseline: no such log: %s\n" % log_path)
        return EXIT_CANNOT_EVALUATE

    run = parse_run(_read(log_path))
    baseline_path = pathlib.Path(args.baseline)

    if args.command == "accept":
        plan = args.plan or "PlayheadFastTests"
        if baseline_path.exists():
            base = load_baseline(baseline_path)
        else:
            base = empty_baseline(plan)
        try:
            merged = merge(base, run, plan=plan)
        except CannotEvaluate as exc:
            sys.stderr.write("gate-baseline: REFUSING to accept — %s\n" % exc)
            sys.stderr.write("gate-baseline: the baseline file was NOT written.\n")
            return EXIT_CANNOT_EVALUATE
        added = sorted(set(merged["tests"]) - set(base.get("tests", {})))
        removed = sorted(set(base.get("tests", {})) - set(merged["tests"]))
        promoted, demoted = tier_changes(base, merged)
        save_baseline(baseline_path, merged)
        print("gate-baseline: wrote %s" % baseline_path)
        print("  plan=%s  observations=%d  known-broken=%d"
              % (merged["plan"], merged["runs_observed"], len(merged["tests"])))
        if added:
            census = kind_census([merged["tests"][key] for key in added])
            print("  added %d: %s" % (
                len(added),
                ", ".join("%d %s" % (census[label], label) for label in sorted(census)),
            ))
        # The KIND rides on every added line. The justification an operator
        # writes for this accept is a claim about these entries, and a claim
        # about their kinds is only checkable if the kinds were on screen.
        for key in added:
            print("  + [%s] %s" % (_kinds_label(merged["tests"][key]), key))
        for key in removed:
            print("  - %s" % key)
        if not added and not removed:
            print("  (membership unchanged; counts updated)")
        # playhead-tl6l: say what the crash cost this observation. An accept is
        # a claim a human signs in a commit message, and "27 of these entries
        # were never actually observed" is part of the claim.
        #
        # playhead-buvn: and say what the RECORD is now, in both directions.
        # This accept is what arms the census — from here on, a name that loses
        # its verdict and is not on this list fails the gate — so the operator
        # signing the commit message has to be shown the list they are arming,
        # exactly as `ARMED:` below shows them the tier promotions.
        was = recorded_census(base if base.get("plan") == plan else {})
        now = recorded_census(merged)
        added_c, reported_c, dropped_c = census_changes(was, now)
        promoted_c, demoted_c = census_tier_changes(was, now)
        if was is None:
            # Deliberately NOT the word `ARMED`, which belongs to tier
            # promotion below and has a rail asserting it appears only for one.
            print(
                "  CENSUS NOW LIVE: %d crashed-host name(s) recorded over 1 "
                "observation, where nothing was recorded before. It takes %d before an "
                "unrecorded casualty can fail the gate — until then they are named and "
                "not fatal. Say in the commit message why this loss is the "
                "pre-existing one."
                % (len(now.tests), MIN_RUNS_FOR_DETERMINISTIC)
            )
            for key in sorted(now.tests)[:_MAX_LISTED]:
                print("  ~ %s" % key)
            if len(now.tests) > _MAX_LISTED:
                print("  ~ … and %d more" % (len(now.tests) - _MAX_LISTED))
        else:
            for key in added_c:
                print("  ~+ NOW LOSES ITS VERDICT  %s" % key)
            for key in reported_c:
                entry = now.tests[key]
                print("  ~= reported again         %d/%d  %s"
                      % (entry["lost_runs"], entry["seen_runs"], key))
            for key in dropped_c:
                print("  ~- dropped (never started this run — renamed, deleted or "
                      "skipped)  %s" % key)
            print(
                "  crashed-host census: %d -> %d name(s) over %d observation(s). It is "
                "UNIONED with counts, like `tests`: a name that came back is DEMOTED, "
                "not deleted, because one quiet run is not evidence a crash is fixed."
                % (len(was.tests), len(now.tests), now.runs_observed)
            )
        # playhead-o89d R3. This banner used to be spelled `ARMED:` — the SAME
        # four letters as the `tests` promotion 70 lines below, whose consequence
        # is the opposite one (a recorded failure's PASSING becomes fatal; a
        # recorded casualty's REPORTING AT ALL becomes fatal). R2 gave the two
        # DEMOTIONS distinct spellings and wrote the rule down as "neither side
        # can be read as the other", and left the two PROMOTIONS identical.
        # Measured: a mutant that respells this banner as the `tests` one
        # verbatim — telling the operator "Each of these PASSING now fails the
        # gate" about a crashed-host name — SURVIVED the whole suite, as did one
        # that changed this line's own `REPORTING AGAIN` to `PASSES`. So did
        # `ARMED/DISARMED:`, which is THE MUTANT R2 NAMED as its reason for
        # replacing R1's rail: R1's rail was indeed vacuous, and R2's two
        # replacements did not reach this banner either, because both of their
        # scenarios demote rather than promote. Diagnosing a vacuous rail and
        # closing the hole it left open are two jobs. All three are rail-killed
        # now; see `test_the_accept_ANNOUNCES_a_census_promotion…` and RA18/RA19.
        if promoted_c:
            print(
                "  CENSUS ARMED: %d census entr%s crossed into DETERMINISTIC — lost "
                "their verdict in every one of their observations. Each of these "
                "REPORTING AGAIN now fails the gate, which is what lets this record "
                "shrink when the crash is fixed."
                % (len(promoted_c), "y" if len(promoted_c) == 1 else "ies")
            )
            for key in promoted_c:
                entry = now.tests[key]
                print("  !~ now deterministic %d/%d  %s"
                      % (entry["lost_runs"], entry["seen_runs"], key))
        # playhead-o89d R2. The counterpart to the block above, and the same
        # defect R1 fixed one layer up: the census had a loud `ARMED:` for the
        # direction that TIGHTENS and nothing at all for the direction that
        # LOOSENS. A demoted census entry appeared only as `~= reported again
        # 3/4` — a line spelled identically for a LOAD-SENSITIVE entry coming
        # back, which is good news and is not fatal. So the one census event
        # that revokes a hard-failure licence was rendered in the same words as
        # the one that revokes nothing, and the tier had to be re-derived from
        # the counts by a reader who already knew the rule.
        #
        # `CENSUS DISARMED:` rather than a bare `DISARMED:` so the two sides
        # can never be confused for one another: a reader who sees the bare
        # word has watched a recorded FAILURE stop being deterministic, and a
        # reader who sees this one has watched a crashed-host casualty do it.
        if demoted_c:
            print(
                "  CENSUS DISARMED: %d census entr%s LEFT DETERMINISTIC — each was "
                "recorded as losing its verdict in every one of its observations and "
                "this run watched it START AND REPORT, which is what hard-failed the "
                "gate (`REPORTS AGAIN`). Accepting revokes that licence: from here the "
                "entry is load-sensitive and its next report is NOT fatal. That is also "
                "the only way this record ever shrinks, so say in the commit message "
                "whether the crash is FIXED or the RECORD was wrong."
                % (len(demoted_c), "y" if len(demoted_c) == 1 else "ies")
            )
            for key in demoted_c:
                entry = now.tests[key]
                print("  ~! no longer deterministic %d/%d  %s"
                      % (entry["lost_runs"], entry["seen_runs"], key))
        # `CENSUS RECORD ARMED:` rather than `CENSUS ARMED:` (playhead-o89d R3).
        # Its subject is the RECORD, not an entry: it fires once, when the census
        # reaches MIN_RUNS_FOR_DETERMINISTIC observations, and what it arms is the
        # rule for names that are NOT in the record. The banner above is about
        # named entries that ARE. Sharing a spelling made the second look like the
        # counterpart of `CENSUS DISARMED:`, which it is not.
        if was is not None and not was.armed and now.armed:
            print(
                "  CENSUS RECORD ARMED: %d observations recorded. From this accept on, "
                "a test that loses its verdict and is NOT in the record fails the gate."
                % now.runs_observed
            )
        no_verdict = run.no_verdict
        if no_verdict:
            protected = sorted(set(no_verdict) & set(merged["tests"]))
            print(
                "  NO VERDICT: the host died and %d test(s) reported nothing%s. This "
                "observation says nothing about them."
                % (len(no_verdict),
                   " (xcodebuild restarted the test host %d time(s))" % run.host_restarts
                   if run.host_restarts else "")
            )
            if protected:
                print(
                    "  CARRIED FORWARD: %d recorded entr%s kept unchanged rather than "
                    "pruned — a crash is not a rename, and dropping them here is how "
                    "the file would shrink without anyone deciding to shrink it."
                    % (len(protected), "y was" if len(protected) == 1 else "ies were")
                )
                for key in protected[:_MAX_LISTED]:
                    print("  = %s" % key)
                if len(protected) > _MAX_LISTED:
                    print("  = … and %d more" % (len(protected) - _MAX_LISTED))
        if promoted:
            print(
                "  ARMED: %d entr%s crossed into DETERMINISTIC — failed in every one "
                "of their observations. Each of these PASSING now fails the gate, so "
                "say in the commit message why that is the right reading."
                % (len(promoted), "y" if len(promoted) == 1 else "ies")
            )
            for key in promoted:
                entry = merged["tests"][key]
                print("  ! now deterministic [%s] %d/%d  %s" % (
                    _kinds_label(entry), entry["failed_runs"], entry["seen_runs"], key,
                ))
        # playhead-o89d review. The counterpart to `ARMED:` above, and it used to
        # be four bare words — `~ no longer deterministic <key>` — for the single
        # event this module most wants a human to think about.
        #
        # A `tests` demotion has exactly one cause: the entry was recorded as
        # failing in EVERY observation, this run watched it PASS, and that pass is
        # what put `NOW PASSES` on the verdict and HARD-FAILED the gate. Accepting
        # revokes the licence — the entry becomes load-sensitive and its next pass
        # is no longer fatal. That is the gate getting LOOSER, which is precisely
        # the direction that has to be justified out loud.
        #
        # The asymmetry was not theoretical: this bead's own accept demoted a 4/4
        # entry and its commit message described it as "the pass-direction arm
        # doing its job on a LOAD-SENSITIVE entry: reported, not fatal" — the AFTER
        # tier read as though it were the BEFORE tier, on an event that was in fact
        # fatal. So the line now names the tier it is leaving, the event, and the
        # consequence, exactly as the promotion line does.
        if demoted:
            print(
                "  DISARMED: %d entr%s LEFT DETERMINISTIC — each was recorded as "
                "failing in every one of its observations and this run watched it "
                "PASS, which is what hard-failed the gate (`NOW PASSES`). Accepting "
                "revokes that licence: from here the entry is load-sensitive and its "
                "next pass is NOT fatal. Say in the commit message why the RECORD was "
                "wrong rather than the run."
                % (len(demoted), "y" if len(demoted) == 1 else "ies")
            )
        for key in demoted:
            entry = merged["tests"][key]
            print("  ~ no longer deterministic [%s] %d/%d  %s" % (
                _kinds_label(entry), entry["failed_runs"], entry["seen_runs"], key,
            ))
        return EXIT_OK

    if not baseline_path.exists():
        sys.stderr.write(
            "gate-baseline: CANNOT EVALUATE — no baseline at %s. Create one with "
            "`scripts/fast-gate.sh --accept-baseline`.\n" % baseline_path
        )
        return EXIT_CANNOT_EVALUATE

    base = load_baseline(baseline_path)
    result = verdict(base, run, plan=args.plan)
    print(result.render())
    return result.exit_code


if __name__ == "__main__":
    sys.exit(main())
