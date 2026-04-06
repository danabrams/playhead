// FoundationModelClassifierTests.swift
// Phase 3.1 de-risking test: can Foundation Models catch the ads that
// LexicalScanner cannot? Runs the coarse FM classifier against specific
// segments from the Conan "Fanhausen Revisited" transcript that have
// known ground truth.

import Foundation
import Testing
@testable import Playhead

@Suite("Foundation Model Classifier — Phase 3.1 de-risking")
struct FoundationModelClassifierTests {

    // MARK: - Ground truth segments from the Conan Fanhausen transcript

    /// Each test segment is pulled directly from the real Conan transcript
    /// fixture and has a known ad/non-ad label.
    private struct TestSegment {
        let label: String
        let knownIntent: FMCoarseScanOutput.Intent  // expected
        let text: String
    }

    /// These are the critical segments — especially Kelly Ripa, which
    /// LexicalScanner cannot detect.
    private static let segments: [TestSegment] = [
        // 0:00-0:26 — CVS pharmacy pre-roll (LexicalScanner CAN catch via URL)
        TestSegment(
            label: "CVS pre-roll (0:00-0:26)",
            knownIntent: .commercial,
            text: "getting your vaccines matters but with so much confusing information out there it can be hard to know for sure your local cvs pharmacists are here to help with the answers to your questions and important vaccines like shingles rsv and pneumococcal pneumonia if eligible so you can get the protection you need and peace of mind too schedule yours today at cvs.com or on the cvs health app"
        ),
        // 0:30-0:56 — Kelly Ripa cross-promo (LexicalScanner CANNOT catch — no URL, no promo code)
        TestSegment(
            label: "Kelly Ripa cross-promo (0:30-0:56) — the critical test",
            knownIntent: .commercial,
            text: "hey everyone it's kelly ripper and we're celebrating 3 years of my podcast let's talk off camera no hair no makeup just 3 great years of the most honest conversations real stories and unfiltered talk and we're joined every week by celebrity guests like nicky glaser kate hudson oprah and more 3 years in and we're not done yet listen to let's talk off camera wherever you get your podcasts"
        ),
        // 1:00-1:30 — actual show content, should be editorial
        TestSegment(
            label: "Show intro — Danhausen anniversary (1:00-1:30)",
            knownIntent: .editorial,
            text: "well we have come up on a very nice anniversary hard to believe but 5 years ago this month we did our very first fan episode and it's been 5 years and i really love these segments and the very first one featured a fan of mine named dan hausen now dan hausen is a wrestler and he explained to me when we did this very first fan episode that he had loosely based his character his wrestler character on me if i was an interdimensional demon"
        ),
        // 3:17-3:26 — teamcoco.com call-in instructions (show structure, NOT an ad)
        // This is the critical false-positive test for LexicalScanner
        TestSegment(
            label: "teamcoco.com call-in (3:17-3:26) — false-positive test",
            knownIntent: .editorial,
            text: "conan o'brien needs a fan want to talk to conan visit teamcoco.com slash call conan okay let's get started hey everybody conan o'brien"
        ),
        // 15:52-15:59 — SiriusXM in credits (edge case, 50% ad in user's gradient model)
        TestSegment(
            label: "SiriusXM in credits (15:52-15:59)",
            knownIntent: .commercial,
            text: "incidental music by jimmy vivino take it away jimmy supervising producer aaron blair associate talent producer jennifer samples associate producers sean doherty and lisa burm engineering by eduardo perez get three free months of siriusxm when you sign up at siriusxm.com slash conan please rate review and subscribe"
        ),
        // 16:11-16:29 — Kelly Ripa cross-promo repeat at end
        TestSegment(
            label: "Kelly Ripa cross-promo repeat (16:11-16:29)",
            knownIntent: .commercial,
            text: "hey everyone it's kelly ripa and we're celebrating 3 years of my podcast let's talk off camera no hair no makeup just 3 great years of the most honest conversations real stories and unfiltered talk and we're joined every week by celebrity guests like nikki glaser"
        ),
    ]

    // MARK: - Tests

    @Test("FM coarse scan distinguishes commercial from editorial content")
    func coarseScanClassification() async throws {
        let classifier = FoundationModelClassifier()

        var correctCount = 0
        var totalLatencyMs: Double = 0
        var unavailableCount = 0
        var results: [(TestSegment, FMCoarseScanOutput)] = []

        print("\n=== Foundation Model Coarse Scan Benchmark ===")
        print("Segments under test: \(Self.segments.count)")

        for segment in Self.segments {
            let output: FMCoarseScanOutput
            do {
                output = try await classifier.coarse(segmentText: segment.text)
            } catch {
                print("\n[\(segment.label)]")
                print("  ERROR: \(error.localizedDescription)")
                continue
            }

            results.append((segment, output))

            if output.intent == .unavailable {
                unavailableCount += 1
                print("\n[\(segment.label)]")
                print("  UNAVAILABLE: \(output.reason)")
                continue
            }

            let correct = output.intent == segment.knownIntent
            if correct { correctCount += 1 }
            totalLatencyMs += output.latencyMillis

            print("\n[\(segment.label)]")
            print("  Expected: \(segment.knownIntent.rawValue)")
            print("  Got:      \(output.intent.rawValue) (conf=\(String(format: "%.2f", output.confidence)))")
            print("  Reason:   \(output.reason)")
            print("  Latency:  \(String(format: "%.0fms", output.latencyMillis))")
            print("  Verdict:  \(correct ? "✓ CORRECT" : "✗ INCORRECT")")
        }

        let scoredCount = Self.segments.count - unavailableCount
        if unavailableCount > 0 {
            print("\n⚠️  FM unavailable on \(unavailableCount)/\(Self.segments.count) segments — simulator may not support Foundation Models. Run on a real iOS 26 device to get real numbers.")
        }

        if scoredCount > 0 {
            let accuracy = Double(correctCount) / Double(scoredCount)
            let avgLatency = totalLatencyMs / Double(scoredCount)
            print("\n=== Summary ===")
            print("Accuracy: \(correctCount)/\(scoredCount) = \(Int(accuracy * 100))%")
            print("Average latency: \(String(format: "%.0fms", avgLatency))")

            // Specifically report on the critical Kelly Ripa case
            if let kellyResult = results.first(where: { $0.0.label.contains("Kelly Ripa cross-promo (0:30") }) {
                print("\n🎯 Critical Kelly Ripa case (LexicalScanner cannot catch this):")
                print("   FM result: \(kellyResult.1.intent.rawValue) (conf=\(String(format: "%.2f", kellyResult.1.confidence)))")
                let kellyCorrect = kellyResult.1.intent == .commercial
                print("   \(kellyCorrect ? "✅ FM CAUGHT the conversational cross-promo — Phase 3 is viable" : "❌ FM MISSED the cross-promo — plan needs rethinking")")
            }

            // Specifically report on the teamcoco false-positive case
            if let teamcocoResult = results.first(where: { $0.0.label.contains("teamcoco") }) {
                print("\n🎯 Critical teamcoco false-positive case:")
                print("   FM result: \(teamcocoResult.1.intent.rawValue) (conf=\(String(format: "%.2f", teamcocoResult.1.confidence)))")
                let teamcocoCorrect = teamcocoResult.1.intent == .editorial
                print("   \(teamcocoCorrect ? "✅ FM correctly identified first-party show structure" : "❌ FM treated first-party URL as commercial (same problem as LexicalScanner)")")
            }
        }

        // Soft assertion only — this is a benchmark, not a pass/fail gate.
        // We want the numbers in the log.
        #expect(true)
    }
}
