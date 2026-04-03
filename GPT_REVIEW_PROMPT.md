Carefully review the attached plan (PLAN.md) for Playhead, an on-device AI-powered iOS podcast player. Come up with your best revisions in terms of better architecture, new features, changed features, etc. to make it better, more robust/reliable, more performant, more compelling/useful, etc. For each proposed change, give me your detailed analysis and rationale/justification for why it would make the project better along with the git-diff style change versus the original plan.

Key constraints to respect:
- All transcription and ad classification MUST be on-device (legal requirement — nothing leaves the phone)
- Monetization is free preview + one-time purchase (no subscription, no API costs)
- The design direction ("Quiet Instrument") is locked — don't propose visual changes that contradict it
- MVP scope is ad detection/skip + ad banners only — don't expand MVP scope, but do improve how those features work
