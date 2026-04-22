# narl counterfactual eval — run 20260422-133137-689491

Generated: 2026-04-22T13:31:37.734Z
Schema: v1

## Summary (rollups)

| Show | Config | Episodes | Excluded | Win F1 @ τ=0.3 | @ 0.5 | @ 0.7 | Second-level F1 | LexInj adds | PriorShift adds | Shadow-covered |
|---|---|---|---|---|---|---|---|---|---|---|
| ALL | allEnabled | 3 | 1 | 0.667 | 0.667 | 0.667 | 0.640 | 0 | 2 | 0 |
| ALL | default | 3 | 1 | 0.750 | 0.750 | 0.750 | 0.727 | 0 | 0 | 0 |
| Conan | allEnabled | 1 | 1 | 0.667 | 0.667 | 0.667 | 0.667 | 0 | 0 | 0 |
| Conan | default | 1 | 1 | 0.667 | 0.667 | 0.667 | 0.667 | 0 | 0 | 0 |
| DoaC | allEnabled | 2 | 0 | 0.667 | 0.667 | 0.667 | 0.625 | 0 | 2 | 0 |
| DoaC | default | 2 | 0 | 0.800 | 0.800 | 0.800 | 0.769 | 0 | 0 | 0 |

## Per-episode

| Episode | Podcast | Config | GT | Pred | F1@0.3 | F1@0.5 | F1@0.7 | Sec-F1 |
|---|---|---|---|---|---|---|---|---|
| doac-ep-002 | flightcast:diary-of-a-ceo | default | 2 | 1 | 0.667 | 0.667 | 0.667 | 0.571 |
| doac-ep-002 | flightcast:diary-of-a-ceo | allEnabled | 2 | 1 | 0.667 | 0.667 | 0.667 | 0.571 |
| conan-ep-001 | simplecast:conan-needs-a-friend | default | 1 | 2 | 0.667 | 0.667 | 0.667 | 0.667 |
| conan-ep-001 | simplecast:conan-needs-a-friend | allEnabled | 1 | 2 | 0.667 | 0.667 | 0.667 | 0.667 |
| doac-ep-001 | flightcast:diary-of-a-ceo | default | 1 | 1 | 1.000 | 1.000 | 1.000 | 1.000 |
| doac-ep-001 | flightcast:diary-of-a-ceo | allEnabled | 1 | 2 | 0.667 | 0.667 | 0.667 | 0.667 |

## Excluded episodes

| Episode | Podcast | Reason |
|---|---|---|
| conan-ep-002 | simplecast:conan-needs-a-friend | wholeAssetVeto:conan-ep-002 |

