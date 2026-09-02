# Shipping the cohort build

`playhead-i7kvl.2`. What has to be true before ~20 TestFlight listeners get a
build, and what only Dan can do.

## What the code now guarantees

Three things, each asserted by `CohortBuildContractTests` rather than assumed.
Two of them were not true when this bead was picked up.

| | State before | Now |
| --- | --- | --- |
| **No entitlement gate** on analysis, downloads or skips | The criterion was *vacuous* — the free tier was deferred, so there was no gate to disable and the check passed on a mechanism that does not exist | A directory walk asserts no entitlement decision reaches those paths, so a gate added later fails the build instead of silently costing testers their analysis |
| **Crash/hang pipeline running** | **Dead.** `MetricKitDiagnosticsInstaller.install` had zero production callers — `playhead-jw63.4` shipped the machinery, closed, and nothing ever started it | Installed from `PlayheadApp.init`, so it runs on every launch including a headless wake |
| **Feedback has a destination** | Fine | Pinned, non-empty, asserted |

The existing MetricKit canary checks what the subscriber *reads* and that it
holds a strong reference. All true, all about a component nobody constructed.
That is the shape to watch for: green rails about internals nothing reaches.

## What Dan has to do

1. **Install a current Xcode beta.** App Store Connect accepts only the
   *current* beta SDK for TestFlight, so every new Xcode beta re-breaks upload
   until you update — expect it every 2–4 weeks (`playhead-i94g`).
2. **Publish the two URLs** and fill the four App Store Connect fields. See
   `docs/site/README.md`. External TestFlight will not accept testers without
   the privacy policy URL.
3. **Archive and upload**, then fill Test Information: beta description,
   feedback email, and the privacy policy URL.
4. **Paste the tester note** from `docs/testflight-what-to-test.md` into
   "What to Test".
5. **Verify on a fresh install**, on a device, before inviting anyone:
   - the app analyses an episode with no purchase and no paywall;
   - Settings → Export dogfood logs produces a mail with an attachment;
   - that attachment parses: `python3 scripts/cohort_readout.py <folder>`;
   - a deliberate crash appears in the pipeline (this cannot be checked on the
     simulator — MetricKit never delivers there).
6. **Record the build number** on `docs/cohort-build-flags.md`
   (`playhead-i7kvl.4`), so a tester report can be attributed to a build.

## The one thing that cannot be verified here

Step 5's crash check. `MetricKitDiagnosticsInstaller.install` is compiled out on
the simulator and refuses inside an XCTest host, both deliberately, so the
install is asserted from source and the *delivery* has to be confirmed on real
hardware once. Until somebody does that, "crashes get collected" is a wiring
claim rather than an observed one.
