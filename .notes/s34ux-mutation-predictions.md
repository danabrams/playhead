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

Whole battery on the final tree: **147 mutations, 145 killed, 1 survived (R21,
pre-existing), 0 error, control R99 OK-SURVIVED.**
