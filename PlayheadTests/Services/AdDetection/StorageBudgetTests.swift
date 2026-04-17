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

        // Setup: after an eviction cycle will delete 1 GB of media,
        // retained warmResumeBundle is 20 MB. 20 MB / 1 GB = 2% > 1%
        // max ⇒ breach.
        ledger.set(.media, to: defaultMediaCapBytes + 1_000_000_000) // over by 1 GB
        ledger.set(.warmResumeBundle, to: 20 * 1_000_000)
        ledger.set(.scratch, to: 0)

        let auditCollector = AuditCollector()
        let budget = StorageBudget(
            sizeProvider: { ledger.get($0) },
            evictor: { cls, target in
                ledger.add(cls, -target)
                return target
            },
            auditSink: { auditCollector.append($0) }
        )

        let audit = await budget.enforceCaps()

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

        // First cycle: breach (2% ratio).
        ledger.set(.media, to: defaultMediaCapBytes + 1_000_000_000)
        ledger.set(.warmResumeBundle, to: 20 * 1_000_000)

        let budget = StorageBudget(
            sizeProvider: { ledger.get($0) },
            evictor: { cls, target in
                ledger.add(cls, -target)
                return target
            }
        )

        _ = await budget.enforceCaps()
        #expect(await budget.isWarmResumeAdmissionPaused == true)

        // Second cycle: user deleted the warm bundles manually, and
        // another 1 GB of media goes over the cap. Ratio now 0%,
        // latch should clear.
        ledger.set(.warmResumeBundle, to: 0)
        ledger.set(.media, to: defaultMediaCapBytes + 1_000_000_000)
        _ = await budget.enforceCaps()
        #expect(await budget.isWarmResumeAdmissionPaused == false)
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
}
