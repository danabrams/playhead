# playhead-s34ux — RD-series mutation predictions, WRITTEN BEFORE THE RUN

A mutant reporting KILLED while killing a different test than predicted is a
FALSE CREDIT. The KILLED column alone is not evidence, so the prediction is
recorded first and compared afterwards.

| id | mutation | PREDICTED victims |
|----|----------|-------------------|
| RD01 | SQLite prose leaves the table | Cause.sqlite_cantopen_prose_is_a_resource |
| RD02 | EBADF leaves the errno table | Cause.ebadf_is_a_resource_even_though_the_sentence_is_generic |
| RD03 | unrecognised-errno VETO becomes a search | Cause.an_unrecognised_errno_VETOES_a_recognised_phrase |
| RD05 | console UNANIMITY veto dropped | Console.UNANIMITY_one_real_assertion... + Console.the_veto_holds_whichever_ORDER... |
| RD06 | bundle unanimity becomes a search | Bundle.a_failed_case_with_a_real_assertion_alongside_stays_a_failure |
| RD07 | a message-less FAILED case classified from silence | Bundle.a_failed_case_with_NO_messages_stays_a_failure |
| RD08 | no_verdict stops subtracting resource | Console.it_is_NOT_counted_as_a_crashed_host_casualty + Accept.a_denied_test_does_not_enter_the_crashed_host_census |
| RD09 | host death stops outranking a denial | Bundle.a_crashed_twin_outranks_a_denied_twin_in_BOTH_orders |
| RD10 | collision guard deleted | Bundle.a_passing_same_named_twin_cannot_launder_a_resource_casualty |
| RD11 | headline count is a constant | Verdict.the_count_is_the_NUMBER_of_denied_tests |
| RD12 | resource tail leaves the RED line | Verdict.the_count_rides_on_the_RED_line + Verdict.the_count_is_the_NUMBER_of_denied_tests |
| RD13 | exit_code drops `or self.resource` | Verdict.it_is_NOT_quieter_than_before_the_exit_code_still_says_re_run |
| RD14 | GREEN drops `and not self.resource` | Verdict.GREEN_is_unreachable_while_a_resource_failure_stands |
| RD15 | accept stops protecting denied entries | Accept.a_recorded_entry_denied_this_run_is_NOT_pruned |
| RD16 | BASELINE IS FICTION fires on a denied run | Verdict.BASELINE_IS_FICTION_does_not_fire_on_a_denied_run |
| RD17 | ABSENT member reported as a rename | Verdict.an_ABSENT_baseline_member_names_the_denial_not_a_rename |
| RD18 | console reading survives the bundle | Bundle.the_bundle_REPLACES_the_consoles_reading |
| RD19 | block names a generic cause | Verdict.the_block_NAMES_which_resource_per_test |
| RD20 | block drops what it does NOT know | Verdict.the_block_states_what_it_does_NOT_know |
| RD21 | block prints on every run | Verdict.nothing_is_printed_on_a_healthy_run |

RD13/RD14 and RD12/RD21 are deliberate MIRROR PAIRS: playhead-o89d R5's lesson
is that a rail pinning one direction leaves the other open, and each of these
four needs its own.

R99 (vacuity control) MUST SURVIVE.

## OBSERVED victims — the full suite run under each mutant, restore verified byte-exact

The battery runs only the NAMED tests, so its KILLED column cannot detect a
false credit on its own. This table is the whole 297-test suite run under each
mutant, so the observed victim set is measured rather than assumed.

| id | predicted | observed | verdict |
|----|-----------|----------|---------|
| RD01 | sqlite_cantopen_prose_is_a_resource | 16 test(s) | match (+15, shared fixture) |
| RD02 | ebadf_is_a_resource_even_though_the_sentence_is_generic | 4 test(s) | match (+3, shared fixture) |
| RD03 | an_unrecognised_errno_VETOES_a_recognised_phrase | 1 test(s) | exact match |
| RD05 | UNANIMITY_one_real_assertion_keeps_the_whole_a_failure<br>the_veto_holds_whichever_ORDER_the_issues_arrive_in | 2 test(s) | exact match |
| RD06 | a_failed_case_with_a_real_assertion_alongside_stays_a_failure | 1 test(s) | exact match |
| RD07 | a_failed_case_with_NO_messages_stays_a_failure | 1 test(s) | exact match |
| RD08 | it_is_NOT_counted_as_a_crashed_host_casualty<br>a_denied_does_not_enter_the_crashed_host_census | 4 test(s) | match (+2, shared fixture) |
| RD09 | a_crashed_twin_outranks_a_denied_twin_in_BOTH_orders | 1 test(s) | exact match |
| RD10 | a_GENUINELY_FAILING_twin_outranks_a_denied_twin_in_BOTH_orders | 1 test(s) | exact match |
| RD10b | a_GENUINELY_FAILING_twin_outranks_a_denied_twin_in_BOTH_orders | 1 test(s) | exact match |
| RD10c | a_SKIPPED_twin_does_not_displace_a_denied_twin | 1 test(s) | exact match |
| RD11 | the_count_is_the_NUMBER_of_denied_tests | 1 test(s) | exact match |
| RD12 | the_count_rides_on_the_RED_line<br>the_count_is_the_NUMBER_of_denied_tests | 2 test(s) | exact match |
| RD13 | it_is_NOT_quieter_than_before_the_exit_code_still_says_re_run | 1 test(s) | exact match |
| RD14 | a_long_denied_list_SAYS_that_it_was_truncated | 1 test(s) | exact match |
| RD15 | a_recorded_entry_denied_this_run_is_NOT_pruned | 1 test(s) | exact match |
| RD16 | BASELINE_IS_FICTION_does_not_fire_on_a_denied_run | 1 test(s) | exact match |
| RD17 | an_ABSENT_baseline_member_names_the_denial_not_a_rename | 1 test(s) | exact match |
| RD18 | the_bundle_REPLACES_the_consoles_reading | 1 test(s) | exact match |
| RD19 | the_block_NAMES_which_resource_per_test | 1 test(s) | exact match |
| RD20 | the_block_states_what_it_does_NOT_know | 1 test(s) | exact match |
| RD21 | nothing_is_printed_on_a_healthy_run | 1 test(s) | exact match |

Every single-victim mutant killed EXACTLY the predicted test. The mutants with
extra victims (RD01 +15, RD02 +3, RD08 +2) share a fixture: `CANTOPEN` and
`EBADF_SOURCE` are the messages nearly every rail is built on, so removing them
from the table reaches every rail that uses them. That is over-reach, not false
credit — the predicted victim died in every case.

### The two survivors, and what each one turned out to be

* **RD10 (as first written) SURVIVED and was RIGHT TO.** It exposed a real
  defect: the collision rule had been copied from the crash path, so a DENIAL
  outranked a genuine assertion failure sharing a display name — a real
  regression leaving the NEW column silently. Fixed, re-pointed, and split into
  RD10/RD10b/RD10c so each direction has its own rail.
* **RD14 (as first written) was a PROVEN EQUIVALENT.** `and not self.resource`
  on the GREEN line is unreachable while `exit_code` arms on `self.resource`,
  so removing it changes no output. The clause is kept in source as a stated
  invariant (the precedent is `run.skipped -= …`) and RD14 was re-pointed at a
  live property — the truncation notice on a long denied list.

### One survivor is NOT mine

`R21` — 'the baseline check is applied to selective runs too' — survives on
`105efd5f` as well, verified by running the battery against a clean `git
archive` of that commit. Pre-existing coverage hole, filed rather than fixed.

Whole battery at the time of writing: **147 mutations, 145 killed, 1 survived
(R21, pre-existing), 0 error, control R99 OK-SURVIVED** — measured at `d8f4e7e4`.
**That sentence used to say "on the final tree" and it was wrong twice over:**
review round 1 added 21 more mutants and moved four anchors, after which the
battery REFUSED TO START on the very tree the sentence claimed. See the stamped
figure at the end of this file, and stamp any figure you add — a headline that
names a tree it was not measured on is this bead's own subject.

---

## Correction: the MV1 row in commit `9ad971cb` is MISLABELLED, not over-broad

That commit records:

> `MV1 fd_line returns ""` — predicted 7 victims, OBSERVED 5 — **PREDICTION WAS
> OVER-BROAD**: the two NOT-RECORDED rails assert that text, which the mutant
> makes universal, so they pass.

**Every part of that is wrong except the number 5.** Found by audit, re-measured
here, and the two mutations are genuinely different:

| edit actually applied | what it does | victims |
|---|---|---|
| `if not stats.get("has_fds") … :` → `if True:` | the NOT-RECORDED branch fires **always**, so that text is universal | **the 5 that were observed** |
| the whole guard **and its return** replaced by `return ""` | `fd_line` emits **nothing at all** | **all 7** — the prediction was right |

The commit ran the first and described the second. So the prediction was not
over-broad, the reasoning offered for the discrepancy ("the mutant makes the
text universal") describes the mutation that was RUN rather than the one that
was NAMED, and calling it a wrong prediction credited the ledger with a
self-correction it had not made.

Re-measured on today's tree (the suite has since grown from 7 fd rails to 36, so
the absolute counts move but the split does not): `return ""` kills **12**,
guard-true kills **10**. Restore verified byte-exact both times.

**Both are kept, as a MIRROR PAIR**, because they interrogate different
properties — "does the line exist at all" and "does the NOT-RECORDED branch
discriminate" — and a rail set that only sees one of them has the hole the other
covers.

**The lesson is the one this bead keeps re-learning at a different scale.** A
mutation ledger's whole value is that the label and the edit are the same thing.
Here the label said one edit, the diff said another, the observed victims matched
the diff, and the discrepancy was then explained away in prose — which is a false
credit arriving through the back door, in the very table built to prevent them.
**Read the edit, not the description of the edit.**


---

# The battery, on the tree that is being merged

Run against a **clean `git archive`** of `19978659` in a scratch directory — not in
the worktree, so no uncommitted edit can contribute:

    168 mutation(s): 166 killed, 1 survived, 0 error, control OK-SURVIVED

The single survivor is **`R21`**, which survives on `105efd5f` too (verified
against a clean archive of that commit) and is filed as `playhead-5c006`. It
predates this bead and this bead does not touch its subject.

**Two of round 1's own mutants had to be closed before this number was true**,
and each is worth more than the number:

- **`RD06` was a PROVEN EQUIVALENT created by the review's own strengthening.**
  Round 1 added a second, deeper unanimity veto to `_bundle_resource` whose
  stack starts from the node's own children — so it re-covers the direct ones,
  and the original mutant (first loop's `return None` → `continue`) stopped
  changing anything observable. Good news about the code, stale news about the
  ledger. Re-pointed, exactly as `RD14` was.
- **`RD34` SURVIVED and the code was innocent.** Its replacement text kept
  `2,560`, `2,539` and the bead id — the exact strings its rail pins — so the
  edit could not kill the test it named. A mutant that cannot fail is a mutant
  that proves nothing, and it reads as a coverage hole. **Read the EDIT, not the
  description of the edit** — which is the third time that sentence has had to
  be written in this bead, after the `MV1` mislabel and the `RD10` false credit.

---

## R1 — the adversarial review round: RD22..RD42

Written before the run, in the same format and for the same reason. Measured
against the WHOLE 332-test suite under each mutant, on a clean `git archive` of
`19978659`, not against the named tests alone.

**Headline for the final tree: 168 mutations, 166 killed, 1 survived, 0 error,
control OK-SURVIVED.** The survivor is `R21`, which is pre-existing on
`105efd5f` and filed as `playhead-5c006`. (The R1 round opened at 168 / 164 / 3;
the two extra survivors were `RD06`, a proven equivalent, and `RD34`, a mutant
whose edit did not do what its description said. Both closed — see `19978659`.)

| id | PREDICTED victims | OBSERVED | verdict |
|----|-------------------|----------|---------|
| RD22 | ApplyBundles: SURVIVES_apply_bundles + NOT_reported_as_crashed + does_NOT_cover + conservation | the same 4 | exact |
| RD23 | PASSES_CLEARS + FAILS_outranks + BUNDLE_overrules | the first 2 | OVER-PREDICTED |
| RD24 | restart_and_denial_BOTH | + ONE_spelling | under-predicted |
| RD25 | blamed_and_denial_BOTH | the same 1 | exact |
| RD26 | census_and_denial_BOTH | the same 1 | exact |
| RD27 | restart_BOTH + blamed_BOTH | + ONE_spelling | under-predicted |
| RD28 | ONE_spelling | the same 1 | exact |
| RD29 | denied_run_does_NOT_demote | the same 1 | exact |
| RD30 | genuinely_reported_is_STILL_demoted | + 5 pre-existing census rails | under-predicted |
| RD31 | denied_entry_does_not_HARD_FAIL | the same 1 | exact |
| RD32 | genuine_recovery_reads_as_removal_candidate | + ArmedCensus.NEVER_STARTED | under-predicted |
| RD33 | REPORTED_AGAIN_names_the_DENIAL | the same 1 | exact |
| RD34 | block_NAMES_the_measured_limit | the same 1 | exact (after the mutant was rewritten) |
| RD35 | block_no_longer_claims_RULED_OUT | the same 1 | exact |
| RD36 | NESTED_assertion_vetoes | + Bundle.real_assertion_alongside | under-predicted |
| RD37 | NESTED_denial_does_not_PROMOTE | the same 1 | exact |
| RD38 | DENIAL_ONLY_announces + listing_NAMES_which_cause | the first only | OVER-PREDICTED |
| RD39 | MIXED_counts_BOTH_causes | + the other 2 announcement rails | under-predicted |
| RD40 | listing_NAMES_which_cause | the same 1 | exact |
| RD41 | DENIAL_ONLY_says_a_resource_was_denied | the same 1 | exact |
| RD42 | EBADF_names_the_CEILING | the same 1 | exact |

**Zero false credits: every DECLARED victim actually failed, checked
mechanically over all 25 declared ids.** Thirteen predictions were exact, six
under-predicted (the mutant reached rails I had not thought of, which is
evidence those rails are live rather than a problem), and two OVER-predicted —
both worth reading, because an over-prediction is the half that looks like a
coverage hole and is not:

- **RD23** does not reach `test_the_BUNDLE_still_overrules_the_consoles_reading`
  because that rail applies ONE bundle, and the discard loop RD23 mutates only
  runs when a LATER bundle re-judges a key an earlier one saw. The rail is
  about the console/bundle precedence, not about bundle-to-bundle precedence.
- **RD38** does not reach `test_the_listing_NAMES_which_cause_protected_each_entry`
  because that rail's fixture is a MIXED run, which has casualties, so the
  block RD38 re-nests under `if no_verdict:` still prints. The one rail that
  can see RD38 is the DENIAL-ONLY one, which is exactly the case the defect was.
