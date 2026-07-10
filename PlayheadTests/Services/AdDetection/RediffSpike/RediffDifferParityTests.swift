// RediffDifferParityTests.swift
// playhead-xsdz.29: pins that the PRODUCTION `RediffDiffer` matches the
// test-target reference oracle `RediffPrototype` byte-for-byte (fingerprint-
// index level) on every checked-in fixture AND a large deterministic fuzz
// sweep. The prototype + its python-reference fixtures remain the authoritative
// oracle (playhead-xsdz.16); this test is the tripwire that catches any silent
// drift the moment either differ changes.
//
// Also asserts the productionization did not weaken the public surface: the
// production differ exposes the same `findRuns` / `mergeRuns` / `gapsInA` /
// `gapsInB` / `minGapFps` / `confidence` / `rediff` entry points the oracle
// integration consumes.

import Foundation
import Testing
@testable import Playhead

// MARK: - Deterministic PRNG (public-domain SplitMix64 constants)

private struct SplitMix64Parity {
    var state: UInt64
    init(seed: UInt64) { self.state = seed }
    mutating func next() -> UInt64 {
        state &+= 0x9E37_79B9_7F4A_7C15
        var z = state
        z = (z ^ (z >> 30)) &* 0xBF58_476D_1CE4_E5B9
        z = (z ^ (z >> 27)) &* 0x94D0_49BB_1331_11EB
        return z ^ (z >> 31)
    }
    mutating func nextFingerprint(distinct: UInt32) -> UInt32 {
        // Small alphabet forces frequent value collisions → exercises the
        // inverted-index anchor logic and overlapping-A-interval union path.
        UInt32(next() % UInt64(distinct))
    }
}

@Suite("RediffDiffer ↔ RediffPrototype parity (playhead-xsdz.29)")
struct RediffDifferParityTests {

    // MARK: - Result comparison

    /// Assert two results agree at every field the oracle integration reads.
    private func expectEqual(
        _ prod: RediffDiffer.Result,
        _ proto: RediffPrototype.Result,
        _ label: String
    ) {
        #expect(prod.minGapFpsA == proto.minGapFpsA, "minGapFpsA mismatch: \(label)")
        #expect(prod.minGapFpsB == proto.minGapFpsB, "minGapFpsB mismatch: \(label)")
        #expect(prod.alignedSecondsB == proto.alignedSecondsB, "alignedSecondsB mismatch: \(label)")
        #expect(prod.alignedFractionB == proto.alignedFractionB, "alignedFractionB mismatch: \(label)")

        #expect(prod.mergedRuns.count == proto.mergedRuns.count, "mergedRuns count mismatch: \(label)")
        for (p, q) in zip(prod.mergedRuns, proto.mergedRuns) {
            #expect(p.aStart == q.aStart && p.bStart == q.bStart
                && p.length == q.length && p.errors == q.errors, "run mismatch: \(label)")
        }
        expectSlotsEqual(prod.slotsA, proto.slotsA, "slotsA \(label)")
        expectSlotsEqual(prod.slotsB, proto.slotsB, "slotsB \(label)")
    }

    private func expectSlotsEqual(
        _ prod: [RediffDiffer.Slot],
        _ proto: [RediffPrototype.Slot],
        _ label: String
    ) {
        #expect(prod.count == proto.count, "slot count mismatch: \(label)")
        for (p, q) in zip(prod, proto) {
            #expect(p.startFp == q.startFp && p.endFp == q.endFp, "slot fp mismatch: \(label)")
            #expect(p.leftRunFps == q.leftRunFps && p.rightRunFps == q.rightRunFps, "slot flank fp mismatch: \(label)")
            #expect(p.startSeconds == q.startSeconds && p.endSeconds == q.endSeconds, "slot seconds mismatch: \(label)")
            #expect(p.durationSeconds == q.durationSeconds, "slot duration mismatch: \(label)")
            #expect(p.confidence == q.confidence, "slot confidence mismatch: \(label)")
            #expect(p.leftRunSeconds == q.leftRunSeconds && p.rightRunSeconds == q.rightRunSeconds,
                    "slot flank seconds mismatch: \(label)")
        }
    }

    // MARK: - Fixture parity (real python-reference-derived corpus)

    @Test("production differ matches the prototype on all pinned fixtures",
          arguments: [
            "identity",
            "insert-in-a",
            "insert-in-b",
            "rotation",
            "real-rotated-pair",
          ])
    func fixtureParity(fixtureName: String) throws {
        let fixture = try RediffSpikeFixtureLoader.load(fixtureName)
        let params = fixture.algorithmParams
        let prod = RediffDiffer.rediff(
            fingerprintA: fixture.fingerprintA,
            secondsPerFpA: fixture.secondsPerFpA,
            fingerprintB: fixture.fingerprintB,
            secondsPerFpB: fixture.secondsPerFpB,
            hammingTol: params.hammingTol,
            minRunLen: params.minRunFps,
            offsetSlack: params.offsetSlack,
            gapDiffSlack: params.gapDiffSlack,
            minAdSeconds: params.minAdSeconds
        )
        let proto = RediffPrototype.rediff(
            fingerprintA: fixture.fingerprintA,
            secondsPerFpA: fixture.secondsPerFpA,
            fingerprintB: fixture.fingerprintB,
            secondsPerFpB: fixture.secondsPerFpB,
            hammingTol: params.hammingTol,
            minRunLen: params.minRunFps,
            offsetSlack: params.offsetSlack,
            gapDiffSlack: params.gapDiffSlack,
            minAdSeconds: params.minAdSeconds
        )
        expectEqual(prod, proto, "fixture=\(fixtureName)")
    }

    // MARK: - Differential fuzz (deterministic, seed-swept)

    @Test("production differ matches the prototype across 3,000 randomized pairs")
    func fuzzParity() {
        // Sweep seeds and shapes: small alphabets (heavy collisions), varied
        // lengths (incl. empty / tiny), varied secondsPerFp. Any per-field
        // divergence trips `expectEqual`.
        var cases = 0
        for seed in UInt64(1)...UInt64(3000) {
            var rng = SplitMix64Parity(seed: seed)
            let distinct = UInt32(2 + (rng.next() % 40))        // 2..41 distinct values
            let lenA = Int(rng.next() % 260)                    // 0..259
            let lenB = Int(rng.next() % 260)
            let fpA = (0..<lenA).map { _ in rng.nextFingerprint(distinct: distinct) }
            let fpB = (0..<lenB).map { _ in rng.nextFingerprint(distinct: distinct) }
            // secondsPerFp in a plausible chromaprint band, occasionally asymmetric.
            let secA = 0.05 + Double(rng.next() % 200) / 1000.0  // 0.05..0.249
            let secB = (rng.next() % 3 == 0)
                ? 0.05 + Double(rng.next() % 200) / 1000.0
                : secA

            let prod = RediffDiffer.rediff(
                fingerprintA: fpA, secondsPerFpA: secA,
                fingerprintB: fpB, secondsPerFpB: secB)
            let proto = RediffPrototype.rediff(
                fingerprintA: fpA, secondsPerFpA: secA,
                fingerprintB: fpB, secondsPerFpB: secB)
            expectEqual(prod, proto, "fuzz seed=\(seed)")
            cases += 1
        }
        #expect(cases == 3000)
    }

    // MARK: - Splice-composite fuzz (structured insertions with shared content)

    @Test("production differ matches the prototype on structured splice composites")
    func spliceCompositeParity() {
        // Build B by inserting a random "ad" block into a shared content stream,
        // then diff both ways. Exercises the run-anchoring + gap-extraction paths
        // on inputs that actually align, not just noise.
        for seed in UInt64(1)...UInt64(600) {
            var rng = SplitMix64Parity(seed: seed &* 7 &+ 13)
            let distinct = UInt32(8 + (rng.next() % 200))
            let contentLen = 40 + Int(rng.next() % 200)
            let content = (0..<contentLen).map { _ in rng.nextFingerprint(distinct: distinct) }
            let adLen = Int(rng.next() % 80)
            let ad = (0..<adLen).map { _ in rng.nextFingerprint(distinct: distinct) }
            let cut = Int(rng.next() % UInt64(contentLen + 1))
            var b = Array(content[0..<cut])
            b.append(contentsOf: ad)
            b.append(contentsOf: content[cut...])
            let secA = 0.125, secB = 0.125

            let prod = RediffDiffer.rediff(
                fingerprintA: content, secondsPerFpA: secA,
                fingerprintB: b, secondsPerFpB: secB)
            let proto = RediffPrototype.rediff(
                fingerprintA: content, secondsPerFpA: secA,
                fingerprintB: b, secondsPerFpB: secB)
            expectEqual(prod, proto, "splice seed=\(seed)")
        }
    }

    // MARK: - Component-level parity (findRuns / mergeRuns / gap functions)

    @Test("component functions (findRuns/mergeRuns/gapsInA/gapsInB) match the prototype")
    func componentParity() {
        for seed in UInt64(1)...UInt64(400) {
            var rng = SplitMix64Parity(seed: seed &* 31 &+ 5)
            let distinct = UInt32(2 + (rng.next() % 20))
            let fpA = (0..<Int(rng.next() % 200)).map { _ in rng.nextFingerprint(distinct: distinct) }
            let fpB = (0..<Int(rng.next() % 200)).map { _ in rng.nextFingerprint(distinct: distinct) }

            let prodRuns = RediffDiffer.findRuns(fpA: fpA, fpB: fpB)
            let protoRuns = RediffPrototype.findRuns(fpA: fpA, fpB: fpB)
            #expect(prodRuns.count == protoRuns.count, "findRuns count seed=\(seed)")
            for (p, q) in zip(prodRuns, protoRuns) {
                #expect(p.aStart == q.aStart && p.bStart == q.bStart
                    && p.length == q.length && p.errors == q.errors, "findRuns seed=\(seed)")
            }

            let prodMerged = RediffDiffer.mergeRuns(prodRuns)
            let protoMerged = RediffPrototype.mergeRuns(protoRuns)
            #expect(prodMerged.count == protoMerged.count, "mergeRuns count seed=\(seed)")
            for (p, q) in zip(prodMerged, protoMerged) {
                #expect(p.aStart == q.aStart && p.bStart == q.bStart
                    && p.length == q.length && p.errors == q.errors, "mergeRuns seed=\(seed)")
            }

            let prodGapsA = RediffDiffer.gapsInA(runs: prodMerged, totalA: fpA.count, minGapFps: 8)
            let protoGapsA = RediffPrototype.gapsInA(runs: protoMerged, totalA: fpA.count, minGapFps: 8)
            #expect(prodGapsA.count == protoGapsA.count, "gapsInA count seed=\(seed)")
            for (p, q) in zip(prodGapsA, protoGapsA) {
                #expect(p.startFp == q.startFp && p.endFp == q.endFp
                    && p.leftFlankFps == q.leftFlankFps && p.rightFlankFps == q.rightFlankFps,
                    "gapsInA seed=\(seed)")
            }

            let prodGapsB = RediffDiffer.gapsInB(runs: prodMerged, totalB: fpB.count, minGapFps: 8)
            let protoGapsB = RediffPrototype.gapsInB(runs: protoMerged, totalB: fpB.count, minGapFps: 8)
            #expect(prodGapsB.count == protoGapsB.count, "gapsInB count seed=\(seed)")
            for (p, q) in zip(prodGapsB, protoGapsB) {
                #expect(p.startFp == q.startFp && p.endFp == q.endFp
                    && p.leftFlankFps == q.leftFlankFps && p.rightFlankFps == q.rightFlankFps,
                    "gapsInB seed=\(seed)")
            }
        }
    }

    @Test("minGapFps and confidence match the prototype (banker's rounding + flank curve)")
    func scalarParity() {
        for secTimes1000 in stride(from: 10, through: 300, by: 1) {
            let sec = Double(secTimes1000) / 1000.0
            for minAd in [1.0, 5.0, 12.0, 30.0] {
                #expect(RediffDiffer.minGapFps(minAdSeconds: minAd, secondsPerFp: sec)
                    == RediffPrototype.minGapFps(minAdSeconds: minAd, secondsPerFp: sec),
                    "minGapFps sec=\(sec) minAd=\(minAd)")
            }
        }
        for l in stride(from: 0.0, through: 600.0, by: 3.0) {
            for r in stride(from: 0.0, through: 600.0, by: 37.0) {
                #expect(RediffDiffer.confidence(leftRunSeconds: l, rightRunSeconds: r)
                    == RediffPrototype.confidence(leftRunSeconds: l, rightRunSeconds: r),
                    "confidence l=\(l) r=\(r)")
            }
        }
    }
}
