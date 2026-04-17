// StorageBudgetTests.swift
// playhead-h7r: the storage-budget enforcer.
//
// Acceptance matrix from the bead spec:
//   A. Two caps enforced independently (media write at analysis cap
//      does not evict; analysis write at media cap does not evict).
//   B. Eviction order honored under simultaneous-cap pressure (media
//      first, then scratch; warmResumeBundle is NOT directly evicted).
//   C. Warm-resume ratio assertion fires when retained warmResumeBundle
//      bytes exceed 1% of deleted media bytes in the most recent
//      eviction cycle.
//
// Plus additional coverage:
//   D. Admission rejection on media cap with current-plus-proposed.
//   E. Admission rejection on analysis cap for scratch.
//   F. warmResumeBundle admission paused after ratio breach; resumes
//      when a non-breaching cycle clears the latch.

import Foundation
import Testing

@testable import Playhead

@Suite("StorageBudget (playhead-h7r)")
struct StorageBudgetTests {

    // MARK: - Test doubles

    /// Mutable byte ledger backing the `sizeProvider` closure. Tests
    /// mutate it directly to set up scenarios.
    final class FakeSizeLedger: @unchecked Sendable {
        private let lock = NSLock()
        private var bytes: [ArtifactClass: Int64] = [:]

        func set(_ cls: ArtifactClass, to value: Int64) {
            lock.lock()
            defer { lock.unlock() }
            bytes[cls] = value
        }

        func get(_ cls: ArtifactClass) -> Int64 {
            lock.lock()
            defer { lock.unlock() }
            return bytes[cls] ?? 0
        }

        func add(_ cls: ArtifactClass, _ delta: Int64) {
            lock.lock()
            defer { lock.unlock() }
            bytes[cls] = (bytes[cls] ?? 0) + delta
        }
    }

    /// Collector for audit records — a Sendable reference box so we
    /// can mutate across concurrency domains without tripping Swift 6
    /// actor-isolation checks on captured `var` closures.
    final class AuditCollector: @unchecked Sendable {
        private let lock = NSLock()
        private var audits: [StorageBudgetAudit] = []

        func append(_ audit: StorageBudgetAudit) {
            lock.lock()
            defer { lock.unlock() }
            audits.append(audit)
        }

        var count: Int {
            lock.lock()
            defer { lock.unlock() }
            return audits.count
        }
    }

    // MARK: - Latch helpers

    /// Canonical "trip the warm-resume admission latch" cycle used by
    /// several latch-related tests. Sets `media = mediaCap + 1 GB`,
    /// `warmResumeBundle = 20 MB` (⇒ post-eviction ratio 2% > 1% breach),
    /// runs one `enforceCaps()` cycle, and returns the resulting audit.
    ///
    /// Callers that need a different breach magnitude or that diverge in
    /// the *trip* itself (not the post-trip behaviour) should set their
    /// state inline rather than parameterising this helper into
    /// uselessness — the whole point is to dedupe the canonical setup.
    private func tripLatch(
        on budget: StorageBudget,
        ledger: FakeSizeLedger
    ) async -> StorageBudgetAudit {
        ledger.set(.media, to: defaultMediaCapBytes + 1_000_000_000)
        ledger.set(.warmResumeBundle, to: 20 * 1_000_000)
        return await budget.enforceCaps()
    }

    /// Records each eviction call so tests can assert order and targets.
    final class EvictionRecorder: @unchecked Sendable {
        struct Call: Equatable {
            let cls: ArtifactClass
            let targetBytes: Int64
        }
        private let lock = NSLock()
        private var _calls: [Call] = []

        func record(_ cls: ArtifactClass, target: Int64) {
            lock.lock()
            defer { lock.unlock() }
            _calls.append(Call(cls: cls, targetBytes: target))
        }

        var calls: [Call] {
            lock.lock()
            defer { lock.unlock() }
            return _calls
        }
    }

    // MARK: - A. Independent caps

    @Test("Media write at analysis cap does not evict; analysis write at media cap does not evict")
    func independentCaps() async {
        let ledger = FakeSizeLedger()
        let recorder = EvictionRecorder()

        // Set each class to its cap.
        ledger.set(.media, to: defaultMediaCapBytes)
        ledger.set(.warmResumeBundle, to: 100 * 1_000_000) // half the 200MB analysis cap
        ledger.set(.scratch, to: 50 * 1_000_000)

        let budget = StorageBudget(
            sizeProvider: { ledger.get($0) },
            evictor: { cls, target in
                recorder.record(cls, target: target)
                ledger.add(cls, -target)
                return target
            }
        )

        // A media write of 1 byte would push media over its cap;
        // admission must reject BEFORE touching analysis bytes.
        let mediaDecision = await budget.admit(class: .media, sizeBytes: 1)
        #expect(mediaDecision != StorageAdmissionDecision.accept, "Media write at cap should be rejected")
        if case .rejectCapExceeded(let cls, _, _, _) = mediaDecision {
            #expect(cls == .media)
        } else {
            Issue.record("Expected rejectCapExceeded with class=.media, got \(mediaDecision)")
        }
        #expect(recorder.calls.isEmpty, "Admission must not trigger eviction")

        // An analysis-class write (scratch) while media is already at
        // its cap should be evaluated against ONLY the analysis cap.
        // Current analysis = 100 + 50 = 150 MB; plus 10 MB = 160 MB < 200 MB cap.
        let scratchDecision = await budget.admit(class: .scratch, sizeBytes: 10 * 1_000_000)
        #expect(scratchDecision == .accept, "Scratch admission is independent of media cap")
    }

    // MARK: - B. Eviction order under simultaneous-cap pressure

    @Test("Both caps over: media evicted first, then scratch; warmResumeBundle never evicted directly")
    func evictionOrderUnderDualCapPressure() async {
        let ledger = FakeSizeLedger()
        let recorder = EvictionRecorder()

        // Drive BOTH caps over at the same time.
        //   - media = cap + 2 GB over
        //   - warmResumeBundle = 150 MB
        //   - scratch         = 100 MB
        //   => analysis pool = 250 MB > 200 MB cap.
        ledger.set(.media, to: defaultMediaCapBytes + 2 * 1_000_000_000)
        ledger.set(.warmResumeBundle, to: 150 * 1_000_000)
        ledger.set(.scratch, to: 100 * 1_000_000)

        let budget = StorageBudget(
            sizeProvider: { ledger.get($0) },
            evictor: { cls, target in
                recorder.record(cls, target: target)
                // Simulate a perfect evictor.
                ledger.add(cls, -target)
                return target
            }
        )

        _ = await budget.enforceCaps()

        // Order: .media must come before .scratch. warmResumeBundle
        // must NOT appear in the list at all.
        let classes = recorder.calls.map { $0.cls }
        #expect(classes == [.media, .scratch], "Eviction order wrong: \(classes)")
        #expect(!classes.contains(.warmResumeBundle), "warmResumeBundle must not be evicted directly")

        // Targets: media over by 2 GB → target == 2 GB.
        //          analysis pool over by 50 MB → scratch target == 50 MB.
        #expect(recorder.calls[0].targetBytes == 2 * 1_000_000_000)
        #expect(recorder.calls[1].targetBytes == 50 * 1_000_000)
    }

    // MARK: - C. Warm-resume ratio assertion

    @Test("Warm-resume ratio breach: retained warmResumeBundle > 1% of evicted media trips the latch")
    func warmResumeRatioBreachTripsLatch() async {
        let ledger = FakeSizeLedger()
        // scratch defaults to 0 in FakeSizeLedger; tripLatch() handles
        // the canonical media+warm setup (1 GB media over, warm=20MB,
        // ⇒ post-eviction 2% ratio).
        let auditCollector = AuditCollector()
        let budget = StorageBudget(
            sizeProvider: { ledger.get($0) },
            evictor: { cls, target in
                ledger.add(cls, -target)
                return target
            },
            auditSink: { auditCollector.append($0) }
        )

        let audit = await tripLatch(on: budget, ledger: ledger)

        #expect(audit.evictedMediaBytes == 1_000_000_000)
        #expect(audit.retainedWarmResumeBytes == 20 * 1_000_000)
        #expect(audit.warmResumeToMediaRatio != nil)
        if let r = audit.warmResumeToMediaRatio {
            #expect(r > warmResumeToMediaMaxRatio, "Ratio \(r) should exceed cap")
        }
        #expect(audit.ratioExceeded == true)
        #expect(auditCollector.count == 1, "Audit sink should fire once per cycle")

        // Subsequent warmResumeBundle admission must be refused while
        // the latch is set.
        let decision = await budget.admit(class: .warmResumeBundle, sizeBytes: 1)
        if case .rejectWarmResumeRatioExceeded = decision {
            // pass
        } else {
            Issue.record("Expected rejectWarmResumeRatioExceeded, got \(decision)")
        }
    }

    @Test("Warm-resume ratio OK: retained warmResumeBundle ≤ 1% of evicted media leaves latch clear")
    func warmResumeRatioOkNoLatch() async {
        let ledger = FakeSizeLedger()

        // Setup: 1 GB media evicted, retained warmResumeBundle is 5 MB.
        // 5 MB / 1 GB = 0.5% ≤ 1% ⇒ no breach.
        ledger.set(.media, to: defaultMediaCapBytes + 1_000_000_000)
        ledger.set(.warmResumeBundle, to: 5 * 1_000_000)
        ledger.set(.scratch, to: 0)

        let budget = StorageBudget(
            sizeProvider: { ledger.get($0) },
            evictor: { cls, target in
                ledger.add(cls, -target)
                return target
            }
        )

        let audit = await budget.enforceCaps()
        #expect(audit.ratioExceeded == false)

        // Admission not paused.
        let paused = await budget.isWarmResumeAdmissionPaused
        #expect(paused == false)

        let decision = await budget.admit(class: .warmResumeBundle, sizeBytes: 1)
        #expect(decision == .accept)
    }

    @Test("Latch clears on a subsequent non-breaching eviction cycle")
    func latchClearsOnNonBreachingCycle() async {
        let ledger = FakeSizeLedger()
        let budget = StorageBudget(
            sizeProvider: { ledger.get($0) },
            evictor: { cls, target in
                ledger.add(cls, -target)
                return target
            }
        )

        // First cycle: trip the latch (2% ratio).
        _ = await tripLatch(on: budget, ledger: ledger)
        #expect(await budget.isWarmResumeAdmissionPaused == true)

        // Second cycle: user deleted the warm bundles manually, and
        // another 1 GB of media goes over the cap. Ratio now 0%,
        // latch should clear.
        ledger.set(.warmResumeBundle, to: 0)
        ledger.set(.media, to: defaultMediaCapBytes + 1_000_000_000)
        _ = await budget.enforceCaps()
        #expect(await budget.isWarmResumeAdmissionPaused == false)
    }

    // H1 fix: idle-system recovery. The latch must clear when the
    // system has NO media-cap pressure AND warm bundles have drained
    // to zero (e.g. user deleted warm bundles externally, or stopped
    // downloading podcasts). The previous `latchClearsOnNonBreachingCycle`
    // test inadvertently masked this bug by also creating a fresh 1 GB
    // media-cap breach on cycle 2, which routed the clear through the
    // per-cycle branch instead of the no-pressure branch.
    @Test("Latch clears on no-pressure cycle when warm drains to zero")
    func latchClearsWhenWarmDrainsToZero() async {
        let ledger = FakeSizeLedger()
        let budget = StorageBudget(
            sizeProvider: { ledger.get($0) },
            evictor: { cls, target in
                ledger.add(cls, -target)
                return target
            }
        )

        // Cycle 1: trip the latch with a 2% breach.
        _ = await tripLatch(on: budget, ledger: ledger)
        #expect(await budget.isWarmResumeAdmissionPaused == true)

        // Cycle 2: idle. Media is BELOW cap (no eviction will run),
        // warm has drained to zero externally. There is no per-cycle
        // ratio measurement to run, but the no-pressure recovery path
        // must clear the latch so admission isn't permanently rejected.
        ledger.set(.media, to: defaultMediaCapBytes - 1)
        ledger.set(.warmResumeBundle, to: 0)
        let audit = await budget.enforceCaps()

        #expect(audit.evictedMediaBytes == 0, "Sanity: cycle 2 must not evict media")
        #expect(audit.warmResumeToMediaRatio == nil, "Ratio undefined when no media evicted")
        #expect(audit.ratioExceeded == false, "ratioExceeded is per-cycle: no eviction => false")
        #expect(audit.latchHeld == false, "Latch must clear on no-pressure idle cycle with warm=0")
        #expect(await budget.isWarmResumeAdmissionPaused == false)

        // And admission for warmResumeBundle is now permitted again.
        let decision = await budget.admit(class: .warmResumeBundle, sizeBytes: 1)
        #expect(decision == .accept)
    }

    // Cycle-3 coverage gap: complements `latchClearsWhenWarmDrainsToZero`
    // by walking the OPPOSITE intermediate state. The no-pressure clear
    // branch in `enforceCaps()` requires BOTH `mediaBefore <= mediaCap`
    // AND `warmAfter == 0`. If warm bytes are still retained, the latch
    // must persist across the no-pressure cycle and only clear on a
    // later cycle that fully drains warm.
    @Test("Latch persists while warm retained, clears once warm drains")
    func latchPersistsWhileWarmRetained() async {
        let ledger = FakeSizeLedger()
        let budget = StorageBudget(
            sizeProvider: { ledger.get($0) },
            evictor: { cls, target in
                ledger.add(cls, -target)
                return target
            }
        )

        // Cycle 1: trip the latch via real eviction. media is over by
        // 1 GB, warm = 20 MB ⇒ post-eviction ratio 2% > 1% breach.
        let cycle1 = await tripLatch(on: budget, ledger: ledger)
        #expect(cycle1.evictedMediaBytes == 1_000_000_000, "Cycle 1: media must be evicted")
        #expect(cycle1.retainedWarmResumeBytes == 20 * 1_000_000)
        #expect(cycle1.ratioExceeded == true, "Cycle 1: per-cycle breach signals true")
        #expect(cycle1.latchHeld == true, "Cycle 1: breach sets the latch")
        #expect(await budget.isWarmResumeAdmissionPaused == true)

        // Cycle 2: idle. Media is now BELOW cap (no eviction will run),
        // but warm bundles are STILL retained (small but non-zero, e.g.
        // user has not deleted them externally). The no-pressure clear
        // branch requires `warmAfter == 0`, which is NOT satisfied
        // here — so the latch must persist. This is the case the
        // existing `latchClearsWhenWarmDrainsToZero` test does not cover.
        ledger.set(.media, to: defaultMediaCapBytes - 1)
        ledger.set(.warmResumeBundle, to: 5 * 1_000_000)  // small, non-zero
        let cycle2 = await budget.enforceCaps()

        #expect(cycle2.evictedMediaBytes == 0, "Cycle 2: no media pressure ⇒ no eviction")
        #expect(cycle2.warmResumeToMediaRatio == nil, "Cycle 2: ratio undefined (no eviction)")
        #expect(cycle2.ratioExceeded == false, "Cycle 2: per-cycle is false (no measurement)")
        #expect(cycle2.latchHeld == true,
                "Cycle 2: latch must persist while warm > 0 even with no media pressure")
        #expect(await budget.isWarmResumeAdmissionPaused == true)

        // Cycle 3: warm drains to zero externally, still no media
        // pressure. Now the no-pressure clear branch fires: the latch
        // releases and admission is restored.
        ledger.set(.warmResumeBundle, to: 0)
        let cycle3 = await budget.enforceCaps()

        #expect(cycle3.evictedMediaBytes == 0, "Cycle 3: still no media pressure")
        #expect(cycle3.retainedWarmResumeBytes == 0, "Cycle 3: warm has drained")
        #expect(cycle3.latchHeld == false,
                "Cycle 3: no-pressure clear branch fires when warm == 0")
        #expect(await budget.isWarmResumeAdmissionPaused == false)

        // Admission of warmResumeBundle is permitted again.
        let decision = await budget.admit(class: .warmResumeBundle, sizeBytes: 1)
        #expect(decision == .accept)
    }

    // H2 fix: `ratioExceeded` is per-cycle ONLY. A no-eviction cycle
    // that still holds the latch from a prior breach must report
    // ratioExceeded=false (no measurement happened) and latchHeld=true.
    @Test("Audit splits per-cycle ratioExceeded from persistent latchHeld")
    func auditSeparatesPerCycleBreachFromLatchState() async {
        let ledger = FakeSizeLedger()
        // scratch defaults to 0 in FakeSizeLedger.
        let auditCollector = AuditCollector()
        let budget = StorageBudget(
            sizeProvider: { ledger.get($0) },
            evictor: { cls, target in
                ledger.add(cls, -target)
                return target
            },
            auditSink: { auditCollector.append($0) }
        )

        // Cycle 1: trip the latch.
        let firstAudit = await tripLatch(on: budget, ledger: ledger)
        #expect(firstAudit.ratioExceeded == true, "Cycle with breach: ratioExceeded=true")
        #expect(firstAudit.latchHeld == true, "Cycle with breach: latchHeld=true")

        // Cycle 2: no media eviction (already under cap), warm bundles
        // were not removed externally — latch must persist, but the
        // audit must NOT report a per-cycle breach because no ratio
        // was actually evaluated.
        ledger.set(.media, to: defaultMediaCapBytes - 1)
        ledger.set(.warmResumeBundle, to: 20 * 1_000_000)  // unchanged
        let secondAudit = await budget.enforceCaps()

        #expect(secondAudit.evictedMediaBytes == 0)
        #expect(secondAudit.warmResumeToMediaRatio == nil)
        #expect(secondAudit.ratioExceeded == false,
                "No media evicted => no per-cycle breach signal")
        #expect(secondAudit.latchHeld == true,
                "Latch persists across no-pressure cycle when warm > 0")
        #expect(await budget.isWarmResumeAdmissionPaused == true)
    }

    // MARK: - D/E. Admission rejections

    @Test("Admission rejection: media write exceeding cap is rejected pre-start")
    func admissionRejectedOnMediaCap() async {
        let ledger = FakeSizeLedger()
        ledger.set(.media, to: defaultMediaCapBytes - 1000)

        let budget = StorageBudget(
            sizeProvider: { ledger.get($0) },
            evictor: { _, target in target }
        )

        // A 5 KB write would push media to cap + 4000 bytes.
        let decision = await budget.admit(class: .media, sizeBytes: 5000)
        if case .rejectCapExceeded(let cls, let cap, let current, let proposed) = decision {
            #expect(cls == .media)
            #expect(cap == defaultMediaCapBytes)
            #expect(current == defaultMediaCapBytes - 1000)
            #expect(proposed == 5000)
        } else {
            Issue.record("Expected rejectCapExceeded, got \(decision)")
        }
    }

    @Test("Admission rejection: scratch write exceeding analysis cap is rejected pre-start")
    func admissionRejectedOnAnalysisCap() async {
        let ledger = FakeSizeLedger()
        ledger.set(.warmResumeBundle, to: 150 * 1_000_000)
        ledger.set(.scratch, to: 60 * 1_000_000)
        // Analysis pool already 210 MB > 200 MB cap. Another write must
        // be rejected.

        let budget = StorageBudget(
            sizeProvider: { ledger.get($0) },
            evictor: { _, target in target }
        )

        let decision = await budget.admit(class: .scratch, sizeBytes: 1_000_000)
        if case .rejectCapExceeded(let cls, _, _, _) = decision {
            #expect(cls == .scratch)
        } else {
            Issue.record("Expected rejectCapExceeded, got \(decision)")
        }
    }

    // MARK: - F. In-flight-style scenario: partial eviction

    // M2 fix: admission addition must be checked, not wrapping. With
    // `&+` the projected total wraps to a negative number and the
    // `projected > cap` comparison silently returns .accept.
    @Test("Admission rejects an Int64-overflow projection instead of wrapping to accept")
    func admissionRejectsOnInt64Overflow() async {
        let ledger = FakeSizeLedger()
        // Configure the media class to be exactly Int64.max - 1 bytes.
        // A 100-byte write would overflow (Int64.max is the largest
        // representable). With the buggy `&+`, the projected total
        // would wrap negative and silently accept.
        ledger.set(.media, to: Int64.max - 1)

        // Use a deliberately huge media cap so the only thing that
        // could possibly reject the write is overflow detection.
        let budget = StorageBudget(
            mediaCap: Int64.max,
            sizeProvider: { ledger.get($0) },
            evictor: { _, target in target }
        )

        let decision = await budget.admit(class: .media, sizeBytes: 100)
        if case .rejectCapExceeded(let cls, _, _, _) = decision {
            #expect(cls == .media, "Overflowing projection must reject as cap-exceeded")
        } else {
            Issue.record("Expected rejectCapExceeded on Int64 overflow, got \(decision)")
        }
    }

    @Test("Partial eviction: evictor returns fewer bytes than target; latch logic tolerates it")
    func partialEvictionDoesNotCrash() async {
        let ledger = FakeSizeLedger()
        ledger.set(.media, to: defaultMediaCapBytes + 1_000_000_000)

        // Evictor honors only half of the requested target.
        let budget = StorageBudget(
            sizeProvider: { ledger.get($0) },
            evictor: { cls, target in
                let honored = target / 2
                ledger.add(cls, -honored)
                return honored
            }
        )

        let audit = await budget.enforceCaps()
        #expect(audit.evictedMediaBytes == 500_000_000)
        // After partial eviction, media is still over cap; a future
        // cycle must retry. The enforcer does not loop internally —
        // that's the caller's responsibility.
    }
}

@Suite("StorageBudgetSettings (playhead-h7r)")
struct StorageBudgetSettingsTests {
    /// Use a fresh in-memory UserDefaults suite per test so they don't
    /// cross-contaminate.
    private func freshDefaults() -> UserDefaults {
        let suiteName = "StorageBudgetSettingsTests-\(UUID().uuidString)"
        let defaults = UserDefaults(suiteName: suiteName)!
        defaults.removePersistentDomain(forName: suiteName)
        return defaults
    }

    @Test("Default: unset defaults to 10 GB")
    func unsetDefaultsToTenGB() {
        let defaults = freshDefaults()
        let settings = StorageBudgetSettings.load(from: defaults)
        #expect(settings.mediaCapBytes == defaultMediaCapBytes)
    }

    @Test("Save then load round-trips the user value")
    func saveLoadRoundTrip() {
        let defaults = freshDefaults()
        let new = StorageBudgetSettings(mediaCapBytes: 5 * 1_000_000_000)
        new.save(to: defaults)

        let reloaded = StorageBudgetSettings.load(from: defaults)
        #expect(reloaded.mediaCapBytes == 5 * 1_000_000_000)
    }

    @Test("Corrupted / absurdly-low value falls back to default")
    func corruptedValueFallsBack() {
        let defaults = freshDefaults()
        // Simulate a persisted 1 MB value (below the 100 MB sanity floor).
        defaults.set(NSNumber(value: Int64(1_000_000)), forKey: StorageBudgetSettings.mediaCapBytesKey)

        let reloaded = StorageBudgetSettings.load(from: defaults)
        #expect(reloaded.mediaCapBytes == defaultMediaCapBytes)
    }

    // L1 fix: the clamp path also emits an os.Logger warning. We can't
    // intercept os.Logger output from a unit test, but we can at least
    // verify the warning path doesn't crash and still returns the
    // default. Also verifies the "absent key is not corruption"
    // distinction: an unset key should NOT log (it's the first-launch
    // path), but a persisted-low value should.
    @Test("Clamp path logs without crashing and returns default")
    func clampPathDoesNotCrash() {
        // Clamp scenarios: zero, negative, just-below-floor.
        for badValue: Int64 in [0, -1, 1, 99_999_999] {
            let defaults = freshDefaults()
            defaults.set(NSNumber(value: badValue),
                         forKey: StorageBudgetSettings.mediaCapBytesKey)
            let reloaded = StorageBudgetSettings.load(from: defaults)
            #expect(reloaded.mediaCapBytes == defaultMediaCapBytes,
                    "value=\(badValue) should clamp to default")
        }

        // Absent-key path must also not crash and must return default.
        let emptyDefaults = freshDefaults()
        let fromEmpty = StorageBudgetSettings.load(from: emptyDefaults)
        #expect(fromEmpty.mediaCapBytes == defaultMediaCapBytes)
    }

    // Cycle-3 hardening: a non-numeric persisted value at the key
    // (e.g. a stale string from a prior schema or a corrupted plist)
    // must fall back to the default — not crash, not silently accept.
    // The os.Logger warning is observable in Console.app but not from
    // a unit test, so we only assert the fallback semantics here.
    @Test("Corrupt non-numeric persisted value falls back to default")
    func corruptStringValueFallsBackToDefault() {
        let defaults = freshDefaults()
        // Persist a non-numeric string at the key. The cast-to-NSNumber
        // path returns nil; the corrupt-vs-absent split should log and
        // fall through to the default.
        defaults.set("not a number", forKey: StorageBudgetSettings.mediaCapBytesKey)

        let reloaded = StorageBudgetSettings.load(from: defaults)
        #expect(reloaded.mediaCapBytes == defaultMediaCapBytes,
                "Non-numeric persisted value must fall back to default")
    }
}
