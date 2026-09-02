# The two URLs App Store Connect requires

`playhead-i7kvl.6`. Two static pages, no assets, no scripts, no analytics of
their own. They exist because **external TestFlight cannot ship without them**:
App Store Connect requires a Privacy Policy URL in Beta App Information before a
build reaches external testers, and the App Store listing requires both a
privacy policy URL and a support URL.

## What Dan has to do — this is the whole list

Everything else is done. These four steps need an account or a domain and
cannot be done for you.

1. **Pick where they live.** Anything that serves static HTML over HTTPS.
   GitHub Pages on this repo is free and needs no new account: enable Pages,
   serve from `/docs`, and the files land at
   `https://danabrams.github.io/playhead/site/privacy.html`. A custom domain is
   nicer and is not required.
2. **Choose the support address**, then replace `REPLACE_WITH_SUPPORT_ADDRESS`
   in **both** files. It appears twice in `privacy.html` and twice in
   `support.html`. This is also the decision `playhead-qdyj` has been holding:
   the in-app feedback currently mails `d.abrams@icloud.com` from a single
   pinned constant, so either keep that and use it here, or register an alias
   and change the constant too.
3. **Paste the URLs into App Store Connect**, in both places — they are separate
   fields and filling one does not fill the other:
   - *App Information* → Privacy Policy URL, and *App Information* → Support URL
     (the store listing);
   - *TestFlight* → Test Information → Privacy Policy URL, plus the beta
     description and feedback email (external testers).
4. **Read them once before publishing.** They make factual claims about what the
   app does with data. They were written against the code as it stands on
   2026-09-02 and verified point by point (below), but you are the one signing
   them.

## What was verified, and how

A privacy policy is exactly where an unverified claim becomes a real problem, so
every claim in `privacy.html` was checked against the code rather than against
the design documents.

| Claim | How it was checked |
| --- | --- |
| Audio and transcripts never leave the device | No upload path exists; speech recognition is on-device |
| No Playhead server, no accounts | The only network destinations are podcast hosts and Apple's CloudKit |
| Usage counters are not uploaded | `AnalyticsUploadGate.legalSignoffRecorded` is `false` and the production writer is `DisabledAnalyticsRecordWriter` |
| No third-party analytics, ads, tracking or crash SDK | `project.yml` declares one local package (`coreaiModels`); crash reporting is Apple's MetricKit, written to local storage |
| Diagnostic reports carry no content | The builder admits a fixed vocabulary, ids are hashed, and `DiagnosticsBundlePoisonValueTests` fails the build if any free-text marker reaches the encoded report (playhead-g58r) |
| Subscriptions sync to the user's **own** iCloud | `CKContainerCloudKitProvider` uses `container.privateCloudDatabase` |
| Exactly which fields sync | `SubscriptionRecord`: feed URL, title, author, artwork URL, subscribed date, removed flag. `EntitlementRecord`: product id, granted flag, date, opaque device id. No playback positions, no history, no analysis |

**One correction came out of that pass, and it had already reached the store
copy.** The App Store description said *"Nothing is uploaded … there is no
account"*. That is not accurate: the subscription list syncs to the user's own
iCloud account, which is not a Playhead server but is not nothing either.
`docs/app-store-listing.md` is corrected in the same change. The honest claim is
narrower and still strong — audio and transcripts never leave the device, and
there is no Playhead server or account.

## Keeping them true

These pages describe behaviour, so they go stale when behaviour changes. Two
specific tripwires:

- **If `AnalyticsUploadGate.legalSignoffRecorded` is ever flipped to `true`**,
  the "Usage statistics" section becomes false the moment that ships. The
  telemetry envelope's own activation checklist should include updating this
  page.
- **If anything new is added to the CloudKit sync**, the field-by-field table in
  the policy has to grow with it. It lists fields rather than saying "your
  library" precisely so a reader can tell when it is out of date.
