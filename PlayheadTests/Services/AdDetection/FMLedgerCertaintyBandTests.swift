// FMLedgerCertaintyBandTests.swift
// playhead-yx0f — `buildFMLedgerEntries` FABRICATED a `CertaintyBand` out of
// `transcriptQuality` while the model's real band sat unread in the same row.
//
// # What the defect was
//
//     // Map scan result to a certainty band. The coarse scan
//     // carries transcript quality; use it as a band proxy.
//     // Strong quality -> .moderate, degraded -> .weak.
//     let band: CertaintyBand = result.transcriptQuality == .good ? .moderate : .weak
//
// The comment states the premise plainly and the premise is false. Every
// `containsAd` row persists the model's OWN band in `spansJSON` —
// `CoarseSupportSchema.certainty` on a `passA` OBJECT, each refined span's own
// band in a `passB` ARRAY. Measured on the 2026-08-10 device pull
// (`playhead-gate-artifacts/3gzp/ground-truth.sqlite`): 55 of 55 coarse and 11
// of 11 refined `containsAd` rows carry a band, ZERO carry an empty support
// payload, split 40 `strong` / 15 `moderate` at the row level.
//
// Three consequences, and this suite pins one rail per consequence:
//
//   1. `.strong` was UNREACHABLE, so `case .strong: weight = fusionConfig.fmCap`
//      was DEAD CODE at this call site. 40 of the 55 coarse rows — the ones the
//      model graded strong — were weighted at 0.75x fmCap.
//   2. A `moderate` verdict and a `strong` verdict on the SAME clean transcript
//      weighed identically. The band the model emitted was discarded and
//      replaced by a property of the input text.
//   3. The two quantities are not about the same thing. `transcriptQuality` is
//      a deterministic on-device estimate of ASR cleanliness
//      (`TranscriptQualityEstimator`); the band is the model's confidence in
//      ITS OWN verdict. `FoundationModelClassifier` keeps quality OUT of the
//      `@Generable` schema for exactly that reason.
//
// # The standing diagnostic, which is the shape of `theProxyCannotSeeTheModel`
//
// What would the band read if the model had expressed no certainty at all?
// Exactly what it read when the model said `strong` — `.moderate`, as long as
// the transcript was clean. That is `feedback_ask_what_the_quantity_measures`:
// a value that names one thing read as though it named another. The rail form
// of it is `bandIsIndependentOfTranscriptQuality`, which sweeps all three
// qualities over ONE payload and demands one answer.
//
// # The payloads are REAL
//
// Every `spansJSON` string in `Persisted` is verbatim from that pull, not a
// synthesised shape. The permissive one matters most: 9 of the pull's 11
// refined `containsAd` spans carry `ownershipInferenceWasSuppressed: true`
// beside a `certainty: "strong"` the RUNNER hardcoded, not the model
// (`PermissiveAdClassifier.makeAnchorlessSpan`). Reading that as the model's
// grade would rebuild this bead's own bug on the population where it bites
// hardest, which is why the fix reuses
// `SemanticSweepMarkComposer.certaintyBand(of:)` instead of writing a second
// decoder here.
//
// # What this suite does NOT claim
//
// It says nothing about whether a `strong` verdict is more often right than a
// `moderate` one. The claim is only that the number now reports what it names.

import Foundation
import Testing

@testable import Playhead

// MARK: - Fixtures

private enum Persisted {

    static let assetId = "asset-yx0f"

    /// `passA`, model-graded. Verbatim shapes from the 2026-08-10 pull — note
    /// the two key orders, which are both present in the field and which a
    /// `Decodable` must not care about.
    static let coarseStrong = #"{"supportLineRefs":[17,18,20],"certainty":"strong"}"#
    static let coarseModerate = #"{"supportLineRefs":[61,62],"certainty":"moderate"}"#
    static let coarseStrongKeysSwapped = #"{"certainty":"strong","supportLineRefs":[1,11]}"#

    /// A `weak` coarse grade. Constructed rather than lifted: the pull carries
    /// only `strong` and `moderate` at the row level, and the rung has to be
    /// exercised because it is the one the ABSENCE case shares.
    static let coarseWeak = #"{"supportLineRefs":[4],"certainty":"weak"}"#

    /// `passA` with support but NO grade — the shape a schema change or a
    /// legacy row can produce. The pull has none of these; it is here because
    /// the fallback is a decision this bead had to make, and an untested
    /// decision is a guess.
    static let coarseUngraded = #"{"supportLineRefs":[4]}"#

    /// What `BackfillJobRunner.encodeSupport` writes for a nil `support`.
    static let emptySupport = "[]"

    /// `passB`, model-graded. Verbatim from the pull (one of the 2 of 11
    /// refined spans the model actually graded).
    static let refinedStrong =
        #"[{"anchors":[{"certainty":"strong","evidenceRef":0,"kind":"url","lineRef":2,"#
        + #""resolutionSource":"evidenceRef"},{"certainty":"strong","evidenceRef":1,"#
        + #""kind":"brandSpan","lineRef":2,"resolutionSource":"evidenceRef"}],"#
        + #""certainty":"strong","commercialIntent":"paid","firstLineRef":2,"#
        + #""lastLineRef":2,"ownership":"thirdParty","ownershipInferenceWasSuppressed":false}]"#

    /// `passB`, RUNNER-graded. Verbatim from the pull — 9 of its 11 refined
    /// spans look exactly like this. The `strong` is `PermissiveAdClassifier`'s
    /// hardcode, not the model's grade.
    static let refinedPermissiveStrong =
        #"[{"anchors":[],"certainty":"strong","commercialIntent":"paid","#
        + #""firstLineRef":27,"lastLineRef":29,"ownership":"thirdParty","#
        + #""ownershipInferenceWasSuppressed":true}]"#

    /// Two refined spans, one `strong` and one `moderate`. The WEAKEST governs
    /// because the extent this row backs covers both.
    static func refinedPair(_ first: CertaintyBand, _ second: CertaintyBand) -> String {
        let spans = [first, second].map {
            #"{"anchors":[],"certainty":"\#($0.rawValue)","commercialIntent":"paid","#
                + #""firstLineRef":2,"lastLineRef":2,"ownership":"thirdParty","#
                + #""ownershipInferenceWasSuppressed":false}"#
        }
        return "[\(spans.joined(separator: ","))]"
    }

    /// A graded span beside an UNGRADED one. Nothing may speak for the span
    /// nobody graded.
    static let refinedStrongPlusUngraded =
        #"[{"anchors":[],"certainty":"strong","commercialIntent":"paid","firstLineRef":2,"#
        + #""lastLineRef":2,"ownership":"thirdParty","ownershipInferenceWasSuppressed":false},"#
        + #"{"anchors":[],"commercialIntent":"paid","firstLineRef":4,"lastLineRef":4,"#
        + #""ownership":"thirdParty","ownershipInferenceWasSuppressed":false}]"#

    static let garbage = "not json at all"

    static func row(
        id: String = "scan-yx0f",
        start: Double = 100.0,
        end: Double = 200.0,
        scanPass: String = "passA",
        transcriptQuality: TranscriptQuality = .good,
        disposition: CoarseDisposition = .containsAd,
        spansJSON: String
    ) -> SemanticScanResult {
        SemanticScanResult(
            id: id,
            analysisAssetId: assetId,
            windowFirstAtomOrdinal: 0,
            windowLastAtomOrdinal: 1,
            windowStartTime: start,
            windowEndTime: end,
            scanPass: scanPass,
            transcriptQuality: transcriptQuality,
            disposition: disposition,
            spansJSON: spansJSON,
            status: .success,
            attemptCount: 1,
            errorContext: nil,
            inputTokenCount: nil,
            outputTokenCount: nil,
            latencyMs: nil,
            prewarmHit: false,
            scanCohortJSON: makeCohortJSON(promptLabel: "yx0f"),
            transcriptVersion: "tv-1",
            // playhead-iw7q: EXPLICIT, because the default is now `.unknown`.
            // These fixtures stand for a coarse row the MODEL produced — that is
            // what makes their persisted band attributable at all — and the
            // struct's default deliberately withholds the licence from a writer
            // that says nothing. Saying it here is the fixture doing its job.
            verdictProvenance: .model
        )
    }

    static func span(start: Double = 100.0, end: Double = 200.0) -> DecodedSpan {
        DecodedSpan(
            id: DecodedSpan.makeId(assetId: assetId, firstAtomOrdinal: 0, lastAtomOrdinal: 10),
            assetId: assetId,
            firstAtomOrdinal: 0,
            lastAtomOrdinal: 10,
            startTime: start,
            endTime: end,
            anchorProvenance: []
        )
    }
}

// MARK: - The decoder half (pure, no service)

@Suite("playhead-yx0f — certaintyBand(of:) reads what the model persisted")
struct PersistedCertaintyBandTests {

    @Test("a passA object's `certainty` is the band, in either key order")
    func coarseObjectIsDecoded() {
        #expect(SemanticSweepMarkComposer.certaintyBand(
            of: Persisted.row(spansJSON: Persisted.coarseStrong)) == .strong)
        #expect(SemanticSweepMarkComposer.certaintyBand(
            of: Persisted.row(spansJSON: Persisted.coarseModerate)) == .moderate)
        #expect(SemanticSweepMarkComposer.certaintyBand(
            of: Persisted.row(spansJSON: Persisted.coarseStrongKeysSwapped)) == .strong)
        #expect(SemanticSweepMarkComposer.certaintyBand(
            of: Persisted.row(spansJSON: Persisted.coarseWeak)) == .weak)
    }

    @Test("a passB array is decoded and the WEAKEST span governs")
    func refinedArrayIsDecodedWeakestFirst() {
        #expect(SemanticSweepMarkComposer.certaintyBand(
            of: Persisted.row(scanPass: "passB", spansJSON: Persisted.refinedStrong)) == .strong)
        #expect(SemanticSweepMarkComposer.certaintyBand(
            of: Persisted.row(
                scanPass: "passB",
                spansJSON: Persisted.refinedPair(.strong, .moderate))) == .moderate)
        #expect(SemanticSweepMarkComposer.certaintyBand(
            of: Persisted.row(
                scanPass: "passB",
                spansJSON: Persisted.refinedPair(.moderate, .weak))) == .weak)
    }

    /// The population this bead would have got wrong with a second decoder: 9
    /// of the pull's 11 refined `containsAd` spans are permissive-bypass spans
    /// whose `strong` the runner hardcoded.
    @Test("a RUNNER-hardcoded permissive `strong` is not a band")
    func permissiveFabricationIsUngraded() {
        #expect(SemanticSweepMarkComposer.certaintyBand(
            of: Persisted.row(
                scanPass: "passB",
                spansJSON: Persisted.refinedPermissiveStrong)) == nil)
    }

    /// A graded span must not speak for an ungraded one — the absence sorts
    /// BELOW `.weak`, so the row reports that nobody graded part of it.
    @Test("an ungraded span in an array wins the weakest-governs order")
    func ungradedSpanGovernsTheArray() {
        #expect(SemanticSweepMarkComposer.certaintyBand(
            of: Persisted.row(
                scanPass: "passB",
                spansJSON: Persisted.refinedStrongPlusUngraded)) == nil)
    }

    @Test("no grade at all reads nil, never a band")
    func absentGradeReadsNil() {
        for payload in [Persisted.emptySupport, Persisted.coarseUngraded, Persisted.garbage, "{}"] {
            #expect(
                SemanticSweepMarkComposer.certaintyBand(
                    of: Persisted.row(spansJSON: payload)) == nil,
                "payload \(payload) must not manufacture a band")
        }
    }

    /// THE REFACTOR RAIL. `certaintyFactor(of:)` used to carry this decode in
    /// its own body; playhead-yx0f moved it into `certaintyBand(of:)` and made
    /// the factor delegate. The sweep lane's arithmetic must be byte-identical
    /// across that move, which is what makes this bead a change to ONE lane.
    @Test("certaintyFactor(of:) is exactly certaintyFactor(certaintyBand(of:))")
    func factorIsTheBandsFactor() {
        let payloads = [
            Persisted.coarseStrong, Persisted.coarseModerate, Persisted.coarseWeak,
            Persisted.coarseStrongKeysSwapped, Persisted.coarseUngraded,
            Persisted.emptySupport, Persisted.garbage, "{}",
            Persisted.refinedStrong, Persisted.refinedPermissiveStrong,
            Persisted.refinedStrongPlusUngraded,
            Persisted.refinedPair(.strong, .moderate),
            Persisted.refinedPair(.moderate, .weak),
            Persisted.refinedPair(.strong, .strong),
        ]
        for payload in payloads {
            let row = Persisted.row(spansJSON: payload)
            #expect(
                SemanticSweepMarkComposer.certaintyFactor(of: row)
                    == SemanticSweepMarkComposer.certaintyFactor(
                        SemanticSweepMarkComposer.certaintyBand(of: row)),
                "factor and band disagree on \(payload)")
        }
    }

    /// The absence reads the FLOOR, alongside `.weak` — not the ceiling, and
    /// not the middle rung the old proxy handed out.
    @Test("the ungraded factor is the floor, equal to weak")
    func ungradedFactorIsTheFloor() {
        #expect(SemanticSweepMarkComposer.certaintyFactor(nil)
            == SemanticSweepMarkComposer.certaintyFactor(.weak))
        #expect(SemanticSweepMarkComposer.certaintyFactor(nil)
            < SemanticSweepMarkComposer.certaintyFactor(.moderate))
    }
}

// MARK: - The fusion half (the weight the band buys)

@Suite("playhead-yx0f — buildFMLedgerEntries weighs the model's own band")
struct FMLedgerCertaintyBandTests {

    private func makeService() async throws -> AdDetectionService {
        let store = try await makeTestStore()
        return AdDetectionService(
            store: store,
            metadataExtractor: FallbackExtractor(),
            config: AdDetectionConfig(
                candidateThreshold: 0.40,
                confirmationThreshold: 0.70,
                suppressionThreshold: 0.25,
                hotPathLookahead: 90.0,
                detectorVersion: "yx0f-test",
                fmBackfillMode: .full
            )
        )
    }

    /// One row in, one entry out. `.full` contributes to the existing-candidate
    /// ledger; the span covers the row's whole window.
    private func entry(
        for row: SemanticScanResult,
        service: AdDetectionService,
        config: FusionWeightConfig = FusionWeightConfig()
    ) async -> EvidenceLedgerEntry? {
        await service.buildFMLedgerEntries(
            span: Persisted.span(),
            scanResults: [row],
            mode: .full,
            fusionConfig: config
        ).first
    }

    private func band(of entry: EvidenceLedgerEntry?) -> CertaintyBand? {
        guard case .fm(_, let band, _)? = entry?.detail else { return nil }
        return band
    }

    // MARK: Consequence 1 — `.strong` was dead code

    /// THE DEAD-CODE RAIL. The proxy could only ever return `.moderate` or
    /// `.weak`, so `case .strong: weight = fusionConfig.fmCap` was unreachable
    /// from this call site for 40 of the pull's 55 coarse rows. Nothing else in
    /// this suite would go red if `.strong` were merely never produced.
    @Test("a model `strong` reaches the fmCap rung — the branch the proxy could not reach")
    func strongIsReachable() async throws {
        let svc = try await makeService()
        let config = FusionWeightConfig()
        let got = await entry(
            for: Persisted.row(spansJSON: Persisted.coarseStrong),
            service: svc, config: config)

        #expect(band(of: got) == .strong)
        #expect(got?.weight == config.fmCap)
        // And it is strictly more than the proxy's ceiling, which is the whole
        // reach change this bead makes.
        #expect((got?.weight ?? 0) > config.fmCap * 0.75)
    }

    // MARK: Consequence 2 — two verdicts, one weight

    /// Same transcript quality, different model verdicts. Under the proxy these
    /// two rows were indistinguishable.
    @Test("moderate and strong on the SAME good transcript no longer weigh the same")
    func moderateAndStrongSeparate() async throws {
        let svc = try await makeService()
        let config = FusionWeightConfig()

        let strong = await entry(
            for: Persisted.row(id: "a", spansJSON: Persisted.coarseStrong),
            service: svc, config: config)
        let moderate = await entry(
            for: Persisted.row(id: "b", spansJSON: Persisted.coarseModerate),
            service: svc, config: config)

        #expect(band(of: strong) == .strong)
        #expect(band(of: moderate) == .moderate)
        #expect(strong?.weight == config.fmCap)
        #expect(moderate?.weight == config.fmCap * 0.75)
        #expect((strong?.weight ?? 0) > (moderate?.weight ?? 0))
    }

    // MARK: Consequence 3 — the quantity read is the one reported

    /// THE STANDING DIAGNOSTIC, as a rail. Sweep every `TranscriptQuality` over
    /// ONE payload: the band and the weight must not move. Under the proxy this
    /// test fails on all three arms, because quality WAS the answer.
    @Test("the band is independent of transcriptQuality")
    func bandIsIndependentOfTranscriptQuality() async throws {
        let svc = try await makeService()
        let config = FusionWeightConfig()

        for payload in [Persisted.coarseStrong, Persisted.coarseModerate, Persisted.coarseWeak] {
            var seen: Set<String> = []
            var weights: Set<Double> = []
            for quality in [TranscriptQuality.good, .degraded, .unusable] {
                let got = await entry(
                    for: Persisted.row(transcriptQuality: quality, spansJSON: payload),
                    service: svc, config: config)
                seen.insert(band(of: got)?.rawValue ?? "nil")
                weights.insert(got?.weight ?? -1)
            }
            #expect(seen.count == 1, "band moved with transcript quality on \(payload): \(seen)")
            #expect(weights.count == 1, "weight moved with transcript quality on \(payload)")
        }
    }

    /// The DOWNWARD direction, which the bead's own summary ("40 rows getting
    /// MORE weight") does not name. A model `weak` on a clean transcript used
    /// to be promoted to `.moderate` by the proxy; it is now weighed weak.
    /// Measured on the pull, 7 of the 66 `containsAd` rows move this way.
    @Test("a model `weak` on a good transcript is DEMOTED, not promoted")
    func weakOnGoodTranscriptIsWeighedWeak() async throws {
        let svc = try await makeService()
        let config = FusionWeightConfig()
        let got = await entry(
            for: Persisted.row(transcriptQuality: .good, spansJSON: Persisted.coarseWeak),
            service: svc, config: config)

        #expect(band(of: got) == .weak)
        #expect(got?.weight == config.fmCap * 0.5)
        #expect((got?.weight ?? 1) < config.fmCap * 0.75)
    }

    // MARK: The fallback decision (acceptance criterion 2)

    /// THE FALLBACK RAIL. A payload with no band must read `.weak` — the FLOOR
    /// — and must NOT read the old proxy. The two are distinguishable only on a
    /// `good` transcript, which is exactly where the proxy said `.moderate`, so
    /// that is the arm this test drives.
    @Test("a payload with NO band reads .weak, never the old transcript-quality proxy")
    func absentBandFallsToTheFloorNotTheProxy() async throws {
        let svc = try await makeService()
        let config = FusionWeightConfig()

        for payload in [Persisted.emptySupport, Persisted.coarseUngraded, Persisted.garbage, "{}"] {
            let got = await entry(
                for: Persisted.row(transcriptQuality: .good, spansJSON: payload),
                service: svc, config: config)
            #expect(band(of: got) == .weak, "payload \(payload) did not read the floor")
            #expect(got?.weight == config.fmCap * 0.5,
                    "payload \(payload) was weighed above the floor")
            // The proxy's answer for a `good` transcript. If this ever holds,
            // the fallback has been reinstated.
            #expect(got?.weight != config.fmCap * 0.75, "payload \(payload) took the OLD proxy")
        }
    }

    /// The permissive-bypass population, end to end through the ledger: 9 of
    /// the pull's 11 refined `containsAd` spans carry a runner-hardcoded
    /// `strong`, and they must land on the floor rather than the cap.
    @Test("a permissive-bypass `strong` is weighed as UNGRADED, at the floor")
    func permissiveFabricationIsWeighedAtTheFloor() async throws {
        let svc = try await makeService()
        let config = FusionWeightConfig()

        let fabricated = await entry(
            for: Persisted.row(
                scanPass: "passB", spansJSON: Persisted.refinedPermissiveStrong),
            service: svc, config: config)
        let genuine = await entry(
            for: Persisted.row(scanPass: "passB", spansJSON: Persisted.refinedStrong),
            service: svc, config: config)

        #expect(band(of: fabricated) == .weak)
        #expect(fabricated?.weight == config.fmCap * 0.5)
        #expect(band(of: genuine) == .strong)
        #expect(genuine?.weight == config.fmCap)
    }

    // MARK: The band is REPORTED, not just spent

    /// `DecisionLogger` is the live reader of `EvidenceLedgerDetail.fm`'s band;
    /// it is the only place the band survives into telemetry. A fix that got
    /// the weight right and left the detail carrying the proxy would log a
    /// number nobody could reconcile with the row.
    @Test("the ledger DETAIL carries the read band, not the proxy")
    func detailCarriesTheReadBand() async throws {
        let svc = try await makeService()
        let cases: [(String, TranscriptQuality, CertaintyBand)] = [
            (Persisted.coarseStrong, .degraded, .strong),
            (Persisted.coarseModerate, .good, .moderate),
            (Persisted.coarseWeak, .good, .weak),
            (Persisted.emptySupport, .good, .weak),
        ]
        for (payload, quality, expected) in cases {
            let got = await entry(
                for: Persisted.row(transcriptQuality: quality, spansJSON: payload),
                service: svc)
            #expect(band(of: got) == expected,
                    "detail band wrong for \(payload) at quality \(quality)")
        }
    }

    // MARK: Invariants this bead must not have moved

    /// The Positive-Only Rule, the mode gate and the overlap test are all
    /// upstream of the band and must be untouched. Without this, a mutant that
    /// deleted the disposition guard could be scored against a suite that only
    /// ever looks at bands.
    @Test("only overlapping containsAd rows in a contributing mode produce entries")
    func admissionIsUnchanged() async throws {
        let svc = try await makeService()
        let row = Persisted.row(spansJSON: Persisted.coarseStrong)

        // Non-contributing mode.
        #expect(await svc.buildFMLedgerEntries(
            span: Persisted.span(), scanResults: [row],
            mode: .shadow, fusionConfig: FusionWeightConfig()).isEmpty)

        // Non-positive disposition, WITH a band on the payload so the test
        // cannot pass merely because the payload is empty.
        let noAds = Persisted.row(disposition: .noAds, spansJSON: Persisted.coarseStrong)
        #expect(await svc.buildFMLedgerEntries(
            span: Persisted.span(), scanResults: [noAds],
            mode: .full, fusionConfig: FusionWeightConfig()).isEmpty)

        // No time overlap — the span sits entirely before the row's window.
        #expect(await svc.buildFMLedgerEntries(
            span: Persisted.span(start: 0, end: 50), scanResults: [row],
            mode: .full, fusionConfig: FusionWeightConfig()).isEmpty)

        // The control: the same call that the three above refuse.
        #expect(await svc.buildFMLedgerEntries(
            span: Persisted.span(), scanResults: [row],
            mode: .full, fusionConfig: FusionWeightConfig()).count == 1)
    }

    /// `classificationTrust` is a SECOND, independent read of the same column
    /// and this bead did not touch it. The rail exists because the band read
    /// and the trust read now both decode `spansJSON`, and a future
    /// consolidation must not let one swallow the other: a `passA` object has
    /// no `commercialIntent`, so its trust is the 1.0 default, while a `passB`
    /// span's trust multiplies the band's weight.
    @Test("classificationTrust still modulates the band's weight independently")
    func trustStillModulatesIndependently() async throws {
        let svc = try await makeService()
        let config = FusionWeightConfig()

        // `paid` → trust 1.0, so a genuine refined `strong` reaches the cap.
        let paid = await entry(
            for: Persisted.row(scanPass: "passB", spansJSON: Persisted.refinedStrong),
            service: svc, config: config)
        #expect(paid?.classificationTrust == 1.0)
        #expect(paid?.weight == config.fmCap)

        // `organic` → trust 0.15, and the band is untouched by it.
        let organic = Persisted.refinedStrong
            .replacingOccurrences(of: #""commercialIntent":"paid""#,
                                  with: #""commercialIntent":"organic""#)
        let got = await entry(
            for: Persisted.row(scanPass: "passB", spansJSON: organic),
            service: svc, config: config)
        #expect(band(of: got) == .strong)
        #expect(got?.classificationTrust == 0.15)
        // The trust is applied by `BackfillEvidenceFusion.buildLedger`, not
        // here, so the entry's own weight is still the band's.
        #expect(got?.weight == config.fmCap)
    }
}
