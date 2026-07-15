// StingerBankTests.swift
// playhead-l2f.6: decode/validation tests for the bundled StingerBank —
// malformed JSON is rejected loudly with a typed reason — plus a hermetic
// pin of the shipped `Playhead/Resources/StingerBank.json` asset (show
// count, keys, sides, grid values, template lengths) so a regenerated bank
// that drifts from the committed expectations fails on the simulator, not
// in a Catalyst measurement run.

import Foundation
import Testing
@testable import Playhead

@Suite("StingerBank (playhead-l2f.6)")
struct StingerBankTests {

    // MARK: - Fixture builders

    /// Minimal VALID bank payload; tests mutate a copy to prove each
    /// validation rule rejects loudly.
    private static func validPayload() -> [String: Any] {
        [
            "schemaVersion": 1,
            "envelopeHz": 50,
            "pcmSampleRate": 16_000,
            "shows": [
                [
                    "showKeys": ["test-show", "https://feeds.example.com/test-show"],
                    "showName": "Test Show",
                    "pre": [
                        "template": Array(repeating: 1.25, count: 350),
                        "edgeSampleIndex": 300,
                        "edgeOffsetSeconds": 0.14,
                        "confidence": 0.85,
                        "support": 4,
                    ],
                    "podWidthGridSeconds": 30.0,
                ],
            ],
        ]
    }

    private static func data(_ payload: [String: Any]) throws -> Data {
        try JSONSerialization.data(withJSONObject: payload)
    }

    private static func mutatedShow(
        _ mutate: (inout [String: Any]) -> Void
    ) throws -> Data {
        var payload = validPayload()
        var shows = payload["shows"] as! [[String: Any]]
        mutate(&shows[0])
        payload["shows"] = shows
        return try data(payload)
    }

    /// Asserts decode throws `.malformed` and the reason mentions
    /// `fragment` — the "loudly" half of the contract: failures carry
    /// enough context to identify the offending field.
    private static func expectMalformed(
        _ bankData: Data,
        containing fragment: String
    ) {
        do {
            _ = try StingerBank.decode(bankData)
            Issue.record("expected loud malformed rejection mentioning \"\(fragment)\", but decode succeeded")
        } catch let error as StingerBankError {
            guard case .malformed(let reason) = error else {
                Issue.record("expected .malformed, got \(error)")
                return
            }
            #expect(reason.contains(fragment), "reason \"\(reason)\" must mention \"\(fragment)\"")
        } catch {
            Issue.record("expected StingerBankError, got \(error)")
        }
    }

    // MARK: - Valid decode

    @Test("A valid payload decodes with entry lookup on every alias")
    func validPayloadDecodes() throws {
        let bank = try StingerBank.decode(Self.data(Self.validPayload()))
        #expect(bank.schemaVersion == 1)
        #expect(bank.envelopeHz == 50)
        #expect(bank.pcmSampleRate == 16_000)
        #expect(bank.shows.count == 1)

        // Join-key contract: exact match on EITHER alias (corpus slug OR
        // production feed URL); unknown / nil / empty resolve to nothing.
        #expect(bank.entry(forShowKey: "test-show") != nil)
        #expect(bank.entry(forShowKey: "https://feeds.example.com/test-show") != nil)
        #expect(bank.entry(forShowKey: "other-show") == nil)
        #expect(bank.entry(forShowKey: nil) == nil)
        #expect(bank.entry(forShowKey: "") == nil)

        let entry = try #require(bank.entry(forShowKey: "test-show"))
        let pre = try #require(entry.pre)
        #expect(pre.template.count == 350)
        #expect(pre.edgeSampleIndex == 300)
        #expect(pre.edgeOffsetSeconds == 0.14)
        #expect(pre.confidence == 0.85)
        #expect(pre.support == 4)
        // playhead-l2f.6 gate formula: max(0.50, confidence − 0.15).
        #expect(abs(pre.snapGate - 0.70) < 1e-9)
        #expect(entry.post == nil)
        #expect(entry.podWidthGridSeconds == 30.0)

        // The gate floor half of the formula.
        let floorGate = StingerTemplate(
            template: pre.template,
            edgeSampleIndex: 0,
            edgeOffsetSeconds: 0,
            confidence: 0.55,
            support: 2
        )
        #expect(floorGate.snapGate == 0.50)
    }

    @Test("requiredPCMSampleRate matches the analysis pipeline's decode rate")
    func pcmSampleRateMatchesAnalysisPipeline() {
        // `StingerBank.requiredPCMSampleRate` documents that it must equal
        // `AnalysisAudioService.targetSampleRate` (bank templates and the
        // runtime shard PCM must share one acoustic space for NCC parity).
        // Pin the invariant so a retune of either constant fails here
        // instead of silently degrading every snap.
        #expect(
            Double(StingerBank.requiredPCMSampleRate)
                == AnalysisAudioService.targetSampleRate,
            "bank sample rate must equal the analysis shard decode rate"
        )
    }

    // MARK: - Loud rejection of malformed payloads

    @Test("Syntactically invalid JSON is rejected loudly")
    func invalidJSONRejected() {
        Self.expectMalformed(
            Data("{not json".utf8),
            containing: "decode failed"
        )
    }

    @Test("Unsupported schemaVersion is rejected loudly")
    func wrongSchemaVersionRejected() throws {
        var payload = Self.validPayload()
        payload["schemaVersion"] = 2
        Self.expectMalformed(try Self.data(payload), containing: "schemaVersion 2")
    }

    @Test("Wrong envelopeHz is rejected loudly")
    func wrongEnvelopeHzRejected() throws {
        var payload = Self.validPayload()
        payload["envelopeHz"] = 8
        Self.expectMalformed(try Self.data(payload), containing: "envelopeHz 8")
    }

    @Test("Wrong pcmSampleRate is rejected loudly")
    func wrongSampleRateRejected() throws {
        var payload = Self.validPayload()
        payload["pcmSampleRate"] = 8_000
        Self.expectMalformed(try Self.data(payload), containing: "pcmSampleRate 8000")
    }

    @Test("Missing required keys are rejected loudly")
    func missingKeysRejected() throws {
        var payload = Self.validPayload()
        payload.removeValue(forKey: "shows")
        Self.expectMalformed(try Self.data(payload), containing: "decode failed")
    }

    @Test("Empty showKeys are rejected loudly")
    func emptyShowKeysRejected() throws {
        Self.expectMalformed(
            try Self.mutatedShow { $0["showKeys"] = [String]() },
            containing: "empty showKeys"
        )
        Self.expectMalformed(
            try Self.mutatedShow { $0["showKeys"] = ["test-show", ""] },
            containing: "empty showKeys"
        )
    }

    @Test("Duplicate showKeys across entries are rejected loudly")
    func duplicateShowKeysRejected() throws {
        var payload = Self.validPayload()
        var shows = payload["shows"] as! [[String: Any]]
        var second = shows[0]
        second["showName"] = "Other Show"
        second["showKeys"] = ["test-show"] // collides with the first entry
        shows.append(second)
        payload["shows"] = shows
        Self.expectMalformed(try Self.data(payload), containing: "duplicate showKey")
    }

    @Test("A show without any stinger side is rejected loudly")
    func sidelessShowRejected() throws {
        Self.expectMalformed(
            try Self.mutatedShow { $0.removeValue(forKey: "pre") },
            containing: "no stinger sides"
        )
    }

    @Test("Sub-second templates are rejected loudly")
    func shortTemplateRejected() throws {
        Self.expectMalformed(
            try Self.mutatedShow {
                var pre = $0["pre"] as! [String: Any]
                pre["template"] = Array(repeating: 1.0, count: 49)
                pre["edgeSampleIndex"] = 10
                $0["pre"] = pre
            },
            containing: "template length 49"
        )
    }

    @Test("Out-of-range edgeSampleIndex is rejected loudly")
    func outOfRangeEdgeIndexRejected() throws {
        Self.expectMalformed(
            try Self.mutatedShow {
                var pre = $0["pre"] as! [String: Any]
                pre["edgeSampleIndex"] = 350 // == template.count → out of range
                $0["pre"] = pre
            },
            containing: "edgeSampleIndex 350"
        )
    }

    @Test("Out-of-range confidence is rejected loudly")
    func outOfRangeConfidenceRejected() throws {
        Self.expectMalformed(
            try Self.mutatedShow {
                var pre = $0["pre"] as! [String: Any]
                pre["confidence"] = 1.2
                $0["pre"] = pre
            },
            containing: "confidence 1.2"
        )
        Self.expectMalformed(
            try Self.mutatedShow {
                var pre = $0["pre"] as! [String: Any]
                pre["confidence"] = 0.0
                $0["pre"] = pre
            },
            containing: "confidence 0.0"
        )
    }

    @Test("Insufficient support is rejected loudly")
    func insufficientSupportRejected() throws {
        Self.expectMalformed(
            try Self.mutatedShow {
                var pre = $0["pre"] as! [String: Any]
                pre["support"] = 1
                $0["pre"] = pre
            },
            containing: "support 1"
        )
    }

    @Test("Non-positive grid is rejected loudly")
    func nonPositiveGridRejected() throws {
        Self.expectMalformed(
            try Self.mutatedShow { $0["podWidthGridSeconds"] = -30.0 },
            containing: "podWidthGridSeconds"
        )
    }

    @Test("A bundle without the resource reports missingResource")
    func missingResourceReported() {
        do {
            _ = try StingerBank.load(bundle: Bundle())
            Issue.record("expected missingResource, but load succeeded")
        } catch {
            #expect((error as? StingerBankError) == .missingResource, "got \(error)")
        }
    }

    // MARK: - Hermetic pin of the bundled asset

    @Test("Bundled StingerBank.json decodes and matches the committed generation")
    func bundledBankPins() throws {
        // The shipped bank was generated by `scripts/
        // l2f-boundary-stinger-prototype.py --emit-bank` from the gold
        // artifact earaudit-oracle-gold-b77c…ce82.json (full-corpus,
        // 16 kHz). If someone regenerates it, these pins document exactly
        // what shipped and force the change through review.
        let bank = try StingerBank.load()
        #expect(bank.schemaVersion == 1)
        #expect(bank.envelopeHz == 50)
        #expect(bank.pcmSampleRate == 16_000)
        #expect(bank.shows.count == 4, "bank must cover exactly the 4 shows the full-corpus learner qualified")

        // Show order is deterministic (sorted by show_name at emit time).
        let slugs = bank.shows.map { $0.showKeys[0] }
        #expect(slugs == ["morbid", "on-the-media", "smartless", "the-nikki-glaser-podcast"])

        for show in bank.shows {
            // Every entry carries at least one production feed-URL alias
            // alongside the corpus slug (the join-key contract).
            #expect(show.showKeys.count >= 2, "\(show.showName): expected slug + feed URL aliases")
            #expect(
                show.showKeys.dropFirst().allSatisfy { $0.hasPrefix("https://") },
                "\(show.showName): non-slug aliases must be feed URLs"
            )
            // Both sides shipped for all four shows, full-width templates
            // (7s × 50 Hz), pre edge at frame 300 / post edge at frame 50
            // (the offline TEMPLATE_INNER/OUTER geometry).
            let pre = try #require(show.pre, "\(show.showName): pre side expected")
            let post = try #require(show.post, "\(show.showName): post side expected")
            for (side, name) in [(pre, "pre"), (post, "post")] {
                #expect(side.template.count == 350, "\(show.showName) \(name): template must be 350 frames")
                #expect(side.confidence > 0 && side.confidence <= 1)
                #expect(side.support >= 2)
                #expect(abs(side.edgeOffsetSeconds) < 5, "\(show.showName) \(name): learned offset out of sane range")
            }
            #expect(pre.edgeSampleIndex == 300)
            #expect(post.edgeSampleIndex == 50)
        }

        // Grid values: morbid and on-the-media earned the 30s pod grid;
        // smartless and nikki-glaser did not (full-corpus width fractions
        // below the 0.6 on-grid threshold).
        #expect(bank.entry(forShowKey: "morbid")?.podWidthGridSeconds == 30.0)
        #expect(bank.entry(forShowKey: "on-the-media")?.podWidthGridSeconds == 30.0)
        #expect(bank.entry(forShowKey: "smartless")?.podWidthGridSeconds == nil)
        #expect(bank.entry(forShowKey: "the-nikki-glaser-podcast")?.podWidthGridSeconds == nil)

        // The expected-coverage contract from the bead: morbid pre+post+
        // grid and smartless post shipped; TED Business validated OUT
        // (offset spread 4.4s > 2.0s learning gate) so it must NOT ship
        // until a future corpus qualifies it.
        #expect(bank.entry(forShowKey: "ted-business") == nil, "TED Business failed the full-corpus learning gates and must not ship")
    }
}
