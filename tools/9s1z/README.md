# playhead-9s1z — MEASUREMENT ONLY

Nothing in this directory ships. It builds a **macOS host harness around the REAL
`SemanticSweepMarkComposer` source** so options (i)/(ii)/(iii) can be recomposed
over the 2026-08-19 t4 device pull without re-implementing the composer.

Why a host harness rather than a Python model of the composer: playhead-kg6i's
numbers came from a model, and a model is a second spelling of the thing it
measures. Here `SemanticSweepMarkComposer.swift` is compiled verbatim from the
app target; only its supporting *data* declarations are copied, by line range,
by `extract-deps.sh`, and `verify-deps.sh` re-extracts and diffs to prove the
copy is exact.

    ./extract-deps.sh     # regenerate Deps.swift from the app target
    ./verify-deps.sh      # prove Deps.swift is byte-identical to a fresh extract
    ./build.sh            # build the harness
