# Cohort build flags

**Generated — do not edit by hand.** `python3 scripts/build_flag_sheet.py -o docs/cohort-build-flags.md`

Commit: `0aa5e550`

Every value below is read out of the source named beside it, so this sheet cannot drift from the build. A constant that is renamed or deleted makes `--check` fail rather than leaving a blank row.

| Flag | Value | What a listener gets | Declared in |
| --- | --- | --- | --- |
| `isEnabledByDefault` | `true` | Day-0 rediff runs at all — the byte-exact channel that carries the launch promise | `Playhead/Services/AdDetection/RediffRefetch/RediffActivation.swift` |
| `isEnabledByDefault` | `false` | The LAGGED rediff sweep (distinct from day-0) | `Playhead/Services/AdDetection/RediffRefetch/RediffRefetchService.swift` |
| `isEnabledByDefault` | `false` | Asymmetric auto-skip edge padding (playhead-98co, dormant pending corpus growth) | `Playhead/Services/SkipOrchestrator/AutoSkipEdgePadding.swift` |
| `showIndependentSeedMode` | `.auto` | The trust rung a byte-exact detector starts at — why a NEW show can auto-skip on episode one | `Playhead/Services/TrustScoring/SkipDetectorClass.swift` |
| `defaultAutoDismissSeconds` | `8.0` | How long a card stays on screen | `Playhead/Views/Components/AdBannerView.swift` |
| `episodePreparationCompleteThreshold` | `ReachRatio(0.98)` | Ad-scan coverage at which the library shows the ready checkmark | `Playhead/Views/Library/EpisodePreparationReadiness.swift` |
| `legalSignoffRecorded` | `false` | Analytics UPLOAD. Must be false — the cohort is measured from bundles people choose to send | `Playhead/Services/Analytics/AnalyticsRecordWriter.swift` |
| `dayZeroKickoffResumeLimit` | `25` | How many owed day-0 kickoffs one launch re-drives (playhead-jra6) | `Playhead/App/PlayheadRuntime.swift` |
| `dayZeroKickoffResumeGiveUpAfter` | `3` | How many times one episode may be re-driven before the sweep stops | `Playhead/App/PlayheadRuntime.swift` |
| `schedulerEventsCap` | `200` | Scheduler rows in a report. The CENSUS is counted over the whole journal (playhead-yz3o) | `Playhead/Support/Diagnostics/DiagnosticsBundleBuilder.swift` |
| `workJournalTailCap` | `50` | Work-journal rows in a report | `Playhead/Support/Diagnostics/DiagnosticsBundleBuilder.swift` |

## Reading a tester report against this

- A tester who sees **cards but never a skip** is most likely on shows whose network does not vary its ad insertion, so the byte-exact channel finds nothing. Roughly 29% of episodes on the 2026-09-02 pull. Not a skip-path defect.
- A tester whose library shows **no ready checkmarks** is a coverage question, not a detection one: check the threshold row above.
- If `legalSignoffRecorded` is ever `true` here, stop. Nothing should upload, and `docs/site/privacy.html` becomes false the moment it ships.
