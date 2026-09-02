# Ground truth — the owner's by-ear corrections

**This is the only verdict-bearing data the project has.** Every precision and
recall number in `playhead-3gzp`, `playhead-92im`, `playhead-b6r2` and
`playhead-isp5` is computed against it. It cost a human listening to whole
episodes in real time, and it cannot be regenerated from anything — not from a
re-run, not from a fresh device pull, not from the corpus.

Bead: `playhead-078u`.

## Why this directory exists

The corrections used to survive by accident. They lived in `/private/tmp` under
a session UUID that no process owned, beside 32 sibling copies that had already
been truncated to zero bytes; `/private/tmp` is cleared on reboot, and the
device they came from has since been wiped. A round preserved one copy only
because a grep for non-zero SQLite files happened to walk a sibling session
directory.

A single untracked copy on one machine is a stay of execution. Tracking the
reduced form in git is the fix: it is reviewed, diffed, backed up off-machine,
and a truncation fails a test instead of being discovered years later.

## What is here

| File | What it is |
| --- | --- |
| `2026-08-11-owner-corrections.json` | 30 correction events, the 6 episodes they reference, and the 49 detector windows on those episodes |

The full 84 MB source pull is **not** in git. It lives at
`~/playhead-gate-artifacts/3gzp/ground-truth.sqlite`, and its SHA-256 is
recorded both in that directory's `PINS.txt` and inside the fixture's `source`
block, so a future copy can be proved to be the same bytes:

```
bcad2d09c31607ec593adcfe8749c63b063d2624f3cc8970b584d2b128cb7473
```

That file is also the one the bead names as `vtjx.sqlite`; the hashes match
exactly. Its sibling `corrected2.sqlite` was a second pull of the same data
(different bytes, same 33 assets / 30 corrections / 6 markings on F4CE7F47) and
is gone. Nothing is lost with it.

The two `playhead-3gzp` derivation scripts (`derive.py`, `era_split.py`) stay in
that same artifacts directory rather than in `scripts/`. They read
`transcript_chunks` and `semantic_scan_results`, so they cannot run against the
reduced fixture at all — only against the full pull sitting beside them. Moving
them here would put two generically-named, unrunnable scripts into shared
tooling.

## What is deliberately NOT here, and why

**This repository is public** (`danabrams/playhead`). The standing mandate is
that episode content never leaves the device, while *facts* about a show — "there
is a commercial from 91 s to 153 s" — may be shared. A timestamp is a fact. The
sentence spoken at that timestamp is content.

So `evidenceText` is removed **wherever it appears, at any depth**: as a column
on `ad_windows`, and nested inside the JSON that `correction_events.targetRefsJSON`
carries. `transcript_chunks` is never read at all.

That nesting is the trap and it is worth knowing about before you write the next
exporter. The first version of `scripts/export_ground_truth.py` dropped the
column, asserted "no content", passed — and wrote four verbatim ~200-character
transcript quotes into the fixture through the nested path, because the
assertion inspected only the top-level keys of a row. It is the same shape as
`playhead-g58r` one level up, and the same standing defect class: a guard naming
an ABSENCE whose false branch makes no claim.

`scripts/tests/test_export_ground_truth.py` re-introduces that exact payload and
requires the guard to raise, so the rail is proved rather than assumed.

Because every JSON blob is re-serialized with sorted keys after scrubbing, no
blob in the fixture is byte-identical to its database value. Do not quote one as
verbatim.

## Regenerating, or adding a new pull

```bash
python3 scripts/export_ground_truth.py <pull.sqlite> \
    -o TestFixtures/GroundTruth/<date>-<what-it-is>.json
python3 -m unittest scripts.tests.test_export_ground_truth
```

The export is deterministic — same database in, byte-identical file out — so a
diff means the DATA changed and is worth reading.

## The rule going forward

A device pull that carries a human verdict is evidence, and evidence does not
live in a scratchpad. Reduce it here in the same session it is taken. The
counts in `CheckedInFixtureTests` are pinned on purpose: if a regeneration
produces a different number of corrections, the suite fails and somebody has to
say why. That is the whole point.
