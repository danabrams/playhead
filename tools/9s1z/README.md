# tools/9s1z — recompose a device pull against the REAL composer

**This is a measurement instrument. It is not production, and it structurally cannot be:**
`project.yml` sources the app target from `Playhead/` and the test target from `PlayheadTests/`
only, so nothing under `tools/` is compiled into either. It builds with `swiftc` on the host.

    ./verify-deps.sh   # prove Deps.swift is a verbatim extract of the app target
    ./build.sh         # build the host harness (a few seconds; NOT xcodebuild)
    ./recompose > out/t4-recompose.json
    python3 report.py

## What it does

Compiles `Playhead/Services/AdDetection/SemanticSweepMarkComposer.swift` **verbatim** and runs
`compose` over every asset of a device pull's `analysis.sqlite`, reporting marks, marked seconds,
per-asset totals, per-stage traces, and how much of what the device actually wrote a recompose
reproduces. `extract-deps.sh` generates `Deps.swift` by copying the supporting *data*
declarations out of the app target by line range; `verify-deps.sh` re-extracts and diffs, so a
hand edit in the "verbatim" file fails loudly rather than quietly becoming a model.

**That is the whole point.** playhead-kg6i measured this lane with a re-implementation of the
composer, and the re-implementation was subtly behind the code: its quoted baseline of
79 marks / 8048.8 s reproduces here only with stage 6 (`localise`) disabled, while the tree at
the time actually produced 78 / 7224.0. An 825-second error in the headline number of a decision
document, invisible because the instrument and the subject were two spellings of the same thing.

## Why it is kept in-tree, and what that costs

Kept, deliberately:

* Reach is this project's dominant open problem (`hp70` / `z1px` are P0), and every reach
  decision asks the same question — *what would this change do to the marks on a real device
  pull?* This is the only thing in the repo that answers it without a model.
* It earned its keep on the bead that created it: the stale-baseline finding above came from it,
  and so did the discovery that the rejected option **deleted a real ad** rather than merely
  widening three marks.

The cost, stated rather than glossed: **nothing builds it.** It is outside both targets, so the
gate will not tell you when the app's types move underneath it and it stops compiling. Run
`./verify-deps.sh && ./build.sh` before trusting it, and expect to re-extract a line range or two
after a refactor. If that upkeep ever exceeds its value, delete it — the measurements it produced
are committed in `out/` and do not depend on it still building.

## What it can no longer do, and where that went

While playhead-9s1z was open this harness carried a `versionScopePolicy9s1z` switch in the
composer so all three candidate behaviours could be recomposed side by side. **That switch was
temporary and is gone** — the chosen option is now simply what `presenceExtents` does, and a
measurement switch left in production source is how a measurement becomes a shipped behaviour by
accident.

So the three-option comparison is no longer re-runnable from here. It survives as *data*:
`out/findings.md`, `out/t4-recompose.json` and `out/report.txt` are the record, and the branch's
own history (`a6cd9826`, `0d722304`) shows exactly how the switch was applied if it ever has to
be reconstructed.
